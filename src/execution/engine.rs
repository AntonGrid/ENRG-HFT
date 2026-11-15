//! Layer 4 — the executor.
//!
//! One task, one state machine:
//!
//! ```text
//! Signal (trend)   ─┐
//!                   ├─> gate: fresh AI TRADE verdict?
//! AI verdict ───────┘        │
//!                            ├─> risk guard (size, exposure, cooldown, daily loss)
//!                            ├─> broker.place (paper or HTX)
//!                            └─> fills → portfolio/PnL → ATR trail / SL / trend flip
//! ```
//!
//! The executor never blocks the market-data path: it reads the freshest
//! [`MarketSnapshot`] for the symbol it is about to trade (or is holding) from
//! the shared [`MarketBoard`] and only speaks to the exchange from its own task.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use chrono::Utc;
use rust_decimal::{Decimal, RoundingStrategy};
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, MissedTickBehavior};

use crate::ai_brain::AiAction;
use crate::execution::broker::Broker;
use crate::execution::htx_rest::{Balance, HtxRest};
use crate::execution::portfolio::Portfolio;
use crate::execution::risk::{Intent, RiskGuard, RiskLimits};
use crate::execution::{ExecutionError, OrderKind, OrderReport, OrderRequest, OrderStatus, Side};
use crate::scanner::{is_tradable_usdt_symbol, normalize_symbol, Scanner, SpotQuote};
use crate::strategy::{EntryKind, MarketBoard, MarketSnapshot, Signal};

const TICK_INTERVAL: Duration = Duration::from_millis(250);
/// How often a live order is re-queried (HTX rate limits are per second).
const LIVE_STATUS_POLL: Duration = Duration::from_secs(1);
/// How often the live USDT balance is refreshed for the AI context.
const LIVE_BALANCE_POLL: Duration = Duration::from_secs(30);
/// Pause before re-attempting a failed or cancelled exit.
const EXIT_RETRY_DELAY: Duration = Duration::from_secs(2);
/// How often a startup liquidation order is re-queried.
const RECOVERY_FILL_POLL: Duration = Duration::from_millis(500);
/// How long startup recovery waits for a liquidation order before cancelling.
const RECOVERY_FILL_TIMEOUT: Duration = Duration::from_secs(15);
/// HTX rejects spot orders below 1 USDT with `order-value-min-error`.
const HTX_MIN_ORDER_VALUE_USDT: Decimal = Decimal::ONE;
/// The venue error code returned when an order is below that minimum.
const ORDER_VALUE_MIN_ERROR: &str = "order-value-min-error";
/// First delay before retrying a cancel the venue refused; doubles per attempt
/// until [`CANCEL_BACKOFF_MAX`].
const CANCEL_BACKOFF_STEP: Duration = Duration::from_secs(5);
/// Ceiling of the cancel retry backoff. A cancel that keeps failing is a stuck
/// order, not a busy loop to be hammered every tick.
const CANCEL_BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Failed cancel attempts after which entries are halted: an order the venue
/// refuses to cancel is unmanaged exposure and needs an operator.
const CANCEL_ATTEMPT_LIMIT: u32 = 10;

/// What the process is allowed to do with money.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Signals are logged; no order object is even constructed.
    Off,
    /// Fills are simulated against the live top of book.
    Paper,
    /// Real orders, real money. Requires HTX keys and `EXECUTION_MODE=live`.
    Live,
}

impl ExecutionMode {
    pub fn from_env() -> Result<Self, ExecutionError> {
        let raw = std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "paper".to_string());
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "disabled" => Ok(Self::Off),
            "paper" | "sim" | "simulated" | "" => Ok(Self::Paper),
            "live" | "real" => Ok(Self::Live),
            other => Err(ExecutionError::Config(format!(
                "unknown EXECUTION_MODE={other:?} (expected off|paper|live)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Paper => "paper",
            Self::Live => "live",
        }
    }
}

/// Every knob the executor needs, read once at boot.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Target notional per entry, in quote currency (USDT).
    pub order_notional: Decimal,
    pub min_order_notional: Decimal,
    pub max_order_notional: Decimal,
    pub max_position_notional: Decimal,
    /// Non-positive disables the daily loss budget.
    pub max_daily_loss: Decimal,
    /// ATR multiple below the peak that closes the position (chandelier exit).
    pub trail_atr_multiple: Decimal,
    /// Trailing distance (%) used until the ATR of the symbol is warm.
    pub trail_fallback_pct: Decimal,
    /// Hard stop: the emergency brake that always fires first.
    pub stop_loss_pct: Decimal,
    /// Close the position when the trend filter flips against the long.
    pub trend_exit: bool,
    pub cooldown: Duration,
    /// How long an AI verdict stays valid.
    pub gate_ttl: Duration,
    /// How long an unfilled entry order may rest before it is cancelled.
    pub order_ttl: Duration,
    /// Breakout entries take liquidity instead of resting a maker bid: the very
    /// move the signal describes is what leaves a passive bid behind.
    pub breakout_taker: bool,
    /// Ticks above the best bid a pullback entry rests at, so it joins the front
    /// of the queue instead of the back.
    pub maker_offset_ticks: u32,
    /// Cancel a resting entry once the mark runs this many bps above its limit.
    /// Non-positive disables the check.
    pub entry_max_chase_bps: Decimal,
    pub slippage_bps: Decimal,
    pub fee_bps: Decimal,
    /// Fallback venue steps for symbols the scanner has no metadata for.
    pub base_step: Decimal,
    pub price_step: Decimal,
    pub paper_start_cash: Decimal,
}

impl ExecutorConfig {
    pub fn from_env() -> Result<Self, ExecutionError> {
        let config = Self {
            order_notional: env_decimal("EXEC_ORDER_NOTIONAL_USDT", "25")?,
            min_order_notional: env_decimal("EXEC_MIN_NOTIONAL_USDT", "5")?,
            max_order_notional: env_decimal("EXEC_MAX_ORDER_NOTIONAL_USDT", "50")?,
            max_position_notional: env_decimal("EXEC_MAX_POSITION_NOTIONAL_USDT", "50")?,
            max_daily_loss: env_decimal("EXEC_MAX_DAILY_LOSS_USDT", "20")?,
            trail_atr_multiple: env_decimal("EXEC_TRAIL_ATR_MULT", "2")?,
            trail_fallback_pct: env_decimal("EXEC_TRAIL_FALLBACK_PCT", "1.5")?,
            stop_loss_pct: env_decimal("EXEC_STOP_LOSS_PCT", "0.7")?,
            trend_exit: env_bool("EXEC_TREND_EXIT", true),
            cooldown: env_duration("EXEC_COOLDOWN_SECS", 10)?,
            gate_ttl: env_duration("EXEC_GATE_TTL_SECS", 120)?,
            order_ttl: env_duration("EXEC_ORDER_TTL_SECS", 30)?,
            breakout_taker: env_bool("EXEC_BREAKOUT_TAKER", true),
            maker_offset_ticks: env_u32("EXEC_MAKER_OFFSET_TICKS", 1)?.min(10),
            entry_max_chase_bps: env_decimal("EXEC_ENTRY_MAX_CHASE_BPS", "20")?,
            slippage_bps: env_decimal("EXEC_SLIPPAGE_BPS", "5")?,
            fee_bps: env_decimal("EXEC_FEE_BPS", "20")?,
            base_step: env_decimal("EXEC_BASE_STEP", "0.000001")?,
            price_step: env_decimal("EXEC_PRICE_STEP", "0.01")?,
            paper_start_cash: env_decimal("PAPER_START_USDT", "1000")?,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ExecutionError> {
        if self.order_notional <= Decimal::ZERO {
            return Err(ExecutionError::Config(
                "EXEC_ORDER_NOTIONAL_USDT must be positive".into(),
            ));
        }
        if self.min_order_notional <= Decimal::ZERO {
            return Err(ExecutionError::Config(
                "EXEC_MIN_NOTIONAL_USDT must be positive".into(),
            ));
        }
        if self.max_order_notional < self.min_order_notional {
            return Err(ExecutionError::Config(
                "EXEC_MAX_ORDER_NOTIONAL_USDT must be >= EXEC_MIN_NOTIONAL_USDT".into(),
            ));
        }
        if self.order_notional > self.max_order_notional {
            return Err(ExecutionError::Config(
                "EXEC_ORDER_NOTIONAL_USDT exceeds EXEC_MAX_ORDER_NOTIONAL_USDT; every entry would be rejected".into(),
            ));
        }
        if self.order_notional > self.max_position_notional {
            return Err(ExecutionError::Config(
                "EXEC_ORDER_NOTIONAL_USDT exceeds EXEC_MAX_POSITION_NOTIONAL_USDT; every entry would be rejected".into(),
            ));
        }
        if self.max_position_notional < self.min_order_notional {
            return Err(ExecutionError::Config(
                "EXEC_MAX_POSITION_NOTIONAL_USDT must be >= EXEC_MIN_NOTIONAL_USDT".into(),
            ));
        }
        if self.trail_atr_multiple <= Decimal::ZERO
            || self.trail_fallback_pct <= Decimal::ZERO
            || self.stop_loss_pct <= Decimal::ZERO
        {
            return Err(ExecutionError::Config(
                "EXEC_TRAIL_ATR_MULT, EXEC_TRAIL_FALLBACK_PCT and EXEC_STOP_LOSS_PCT must be positive"
                    .into(),
            ));
        }
        if self.trail_fallback_pct <= self.stop_loss_pct {
            return Err(ExecutionError::Config(
                "EXEC_TRAIL_FALLBACK_PCT must exceed EXEC_STOP_LOSS_PCT; without a warm ATR the emergency stop has to bind"
                    .into(),
            ));
        }
        if self.base_step <= Decimal::ZERO || self.price_step <= Decimal::ZERO {
            return Err(ExecutionError::Config(
                "EXEC_BASE_STEP and EXEC_PRICE_STEP must be positive".into(),
            ));
        }
        if self.fee_bps < Decimal::ZERO || self.slippage_bps < Decimal::ZERO {
            return Err(ExecutionError::Config(
                "EXEC_FEE_BPS and EXEC_SLIPPAGE_BPS must not be negative".into(),
            ));
        }
        if self.entry_max_chase_bps < Decimal::ZERO {
            return Err(ExecutionError::Config(
                "EXEC_ENTRY_MAX_CHASE_BPS must not be negative (0 disables it)".into(),
            ));
        }
        Ok(())
    }

    /// Fractional fee (20 bps = 0.002).
    pub fn fee_rate(&self) -> Decimal {
        self.fee_bps / Decimal::from(10_000)
    }
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_decimal(name: &str, default: &str) -> Result<Decimal, ExecutionError> {
    let raw = env_string(name, default);
    Decimal::from_str(&raw).map_err(|error| {
        ExecutionError::Config(format!("{name}={raw:?} is not a decimal: {error}"))
    })
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32, ExecutionError> {
    let raw = env_string(name, &default.to_string());
    raw.parse::<u32>().map_err(|error| {
        ExecutionError::Config(format!("{name}={raw:?} is not a whole number: {error}"))
    })
}

fn env_duration(name: &str, default_secs: u64) -> Result<Duration, ExecutionError> {
    let raw = env_string(name, &default_secs.to_string());
    raw.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|error| {
            ExecutionError::Config(format!(
                "{name}={raw:?} is not a whole number of seconds: {error}"
            ))
        })
}

/// The USDT balance of the live venue account.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LiveBalance {
    available: Decimal,
    frozen: Decimal,
}

impl LiveBalance {
    fn total(self) -> Decimal {
        self.available + self.frozen
    }
}

/// Extracts the USDT row from the venue balance list.
fn usdt_balance(balances: &[Balance]) -> Option<LiveBalance> {
    balances
        .iter()
        .find(|balance| balance.currency.eq_ignore_ascii_case("USDT"))
        .map(|balance| LiveBalance {
            available: balance.available,
            frozen: balance.frozen,
        })
}

/// Equity of the live account: USDT (free or frozen by an order) plus the
/// marked open position.
///
/// A pending entry is already counted in the venue's frozen USDT, so its
/// reserved notional is subtracted before adding the optimistic position;
/// otherwise equity would count the same 25 USDT twice while the maker rests.
fn live_equity(
    balance: LiveBalance,
    qty: Decimal,
    mark: Option<Decimal>,
    pending_cash: Decimal,
) -> Decimal {
    balance.total() - pending_cash + qty * mark.unwrap_or_default()
}

/// Caps the configured entry notional to what the live USDT balance can fund,
/// fee included. With `available=10` and a 20 bps fee the cap is 9.98003992.
fn live_entry_notional(configured: Decimal, available: Decimal, fee_rate: Decimal) -> Decimal {
    let affordable = (available / (Decimal::ONE + fee_rate)).round_dp(8);
    configured.min(affordable)
}

/// `true` when HTX refused an order because its value is below the venue
/// minimum. Retrying such an order can never succeed: the balance is dust.
fn is_order_value_min_error(error: &ExecutionError) -> bool {
    match error {
        ExecutionError::Api { code, message } => {
            code.eq_ignore_ascii_case(ORDER_VALUE_MIN_ERROR)
                || message.to_ascii_lowercase().contains(ORDER_VALUE_MIN_ERROR)
        }
        _ => false,
    }
}

/// `true` when HTX says the order does not exist. Such an order cannot be
/// cancelled and cannot fill any more, so a cancel loop must end instead of
/// retrying it forever.
fn is_order_not_found(error: &ExecutionError) -> bool {
    match error {
        ExecutionError::Api { code, message } => {
            let code = code.to_ascii_lowercase();
            let message = message.to_ascii_lowercase();
            code.contains("order-not-exist")
                || code.contains("order-not-found")
                || message.contains("order-not-exist")
                || message.contains("order not exist")
        }
        _ => false,
    }
}

/// Delay before the next cancel attempt: `CANCEL_BACKOFF_STEP` per failed
/// attempt, capped at [`CANCEL_BACKOFF_MAX`]. Pure so the schedule is testable
/// without a venue.
fn cancel_backoff(attempts: u32) -> Duration {
    CANCEL_BACKOFF_STEP
        .checked_mul(attempts.max(1))
        .unwrap_or(CANCEL_BACKOFF_MAX)
        .min(CANCEL_BACKOFF_MAX)
}

/// One non-USDT balance found on the venue at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveredHolding {
    currency: String,
    symbol: String,
    qty: Decimal,
    available: Decimal,
    mark: Option<Decimal>,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    base_step: Decimal,
    price_step: Decimal,
}

impl RecoveredHolding {
    fn notional(&self) -> Decimal {
        self.mark.unwrap_or_default() * self.qty
    }
}

/// A balance whose whole value is below the venue's minimum order. It cannot
/// be sold and must never be adopted as a position or retried as an exit.
fn is_dust_holding(holding: &RecoveredHolding) -> bool {
    holding
        .mark
        .is_some_and(|_| holding.notional() < HTX_MIN_ORDER_VALUE_USDT)
}

/// Turns venue balances into recovery candidates.
///
/// USDT is skipped (it is the quote currency, not a position). Every other
/// positive balance is kept, even when no USDT market exists, so the audit can
/// report it instead of silently ignoring the asset.
fn recovery_holdings(
    balances: &[Balance],
    quotes: &HashMap<String, SpotQuote>,
    fallback_base_step: Decimal,
    fallback_price_step: Decimal,
) -> Vec<RecoveredHolding> {
    balances
        .iter()
        .filter_map(|balance| {
            let qty = balance.available + balance.frozen;
            if qty <= Decimal::ZERO || balance.currency.eq_ignore_ascii_case("USDT") {
                return None;
            }
            let symbol = normalize_symbol(&format!("{}usdt", balance.currency))?;
            let quote = quotes.get(&symbol);
            Some(RecoveredHolding {
                currency: balance.currency.to_ascii_uppercase(),
                symbol,
                qty,
                available: balance.available,
                mark: quote.and_then(SpotQuote::mark),
                best_bid: quote.and_then(|quote| quote.best_bid),
                best_ask: quote.and_then(|quote| quote.best_ask),
                base_step: quote
                    .map(|quote| quote.base_step)
                    .unwrap_or(fallback_base_step),
                price_step: quote
                    .map(|quote| quote.price_step)
                    .unwrap_or(fallback_price_step),
            })
        })
        .collect()
}

/// Picks the single position to keep under active management.
///
/// Spot is long-only and the executor tracks one position, so the largest
/// balance with a tradable USDT market becomes the position. Everything else is
/// a candidate for liquidation. Preferences, borrowed tokens and any market
/// that [`is_tradable_usdt_symbol`] rejects are never adopted as the position.
fn pick_primary_holding(holdings: &[RecoveredHolding]) -> Option<usize> {
    holdings
        .iter()
        .enumerate()
        .filter(|(_, holding)| {
            holding.mark.unwrap_or_default() > Decimal::ZERO
                && is_tradable_usdt_symbol(&holding.symbol)
        })
        .max_by(|(_, left), (_, right)| {
            left.notional()
                .cmp(&right.notional())
                .then_with(|| right.symbol.cmp(&left.symbol))
        })
        .map(|(index, _)| index)
}

/// A read-only view of the account, shared with the AI layer.
#[derive(Debug, Clone, Default)]
pub struct AccountView {
    pub equity: Option<Decimal>,
    pub cash: Option<Decimal>,
    pub position_symbol: Option<String>,
    pub position_qty: Decimal,
    pub avg_entry: Decimal,
    pub unrealized: Option<Decimal>,
    pub realized_today: Decimal,
    pub trades: u32,
    pub halted: bool,
}

impl AccountView {
    pub fn describe(&self) -> String {
        format!(
            "equity={} cash={} position_symbol={} position_qty={} avg_entry={} unrealized={} realized_today={} trades={} halted={}",
            option_text(self.equity),
            option_text(self.cash),
            self.position_symbol
                .as_deref()
                .map(|symbol| symbol.to_uppercase())
                .unwrap_or_else(|| "-".to_string()),
            self.position_qty,
            self.avg_entry,
            option_text(self.unrealized),
            self.realized_today,
            self.trades,
            if self.halted { "yes" } else { "no" }
        )
    }
}

fn option_text(value: Option<Decimal>) -> String {
    value
        .map(|value| value.round_dp(4).to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Everything the executor can be told.
#[derive(Debug)]
pub enum ExecutionCommand {
    /// A market dislocation from the strategy, already tagged with its symbol.
    Signal(Signal),
    /// A fresh risk verdict from the AI layer, naming every pair it cleared.
    Verdict {
        action: AiAction,
        symbols: Vec<String>,
        reason: String,
    },
    /// Operator kill switch (Ctrl-C, fatal error elsewhere).
    Halt(String),
}

/// The AI gate: entries require a fresh `TRADE` verdict covering that symbol.
#[derive(Debug, Clone)]
struct Gate {
    action: AiAction,
    symbols: Vec<String>,
    reason: String,
    at: Option<Instant>,
}

impl Gate {
    fn closed() -> Self {
        Self {
            action: AiAction::Pause,
            symbols: Vec::new(),
            reason: "no AI verdict yet".into(),
            at: None,
        }
    }

    fn permits(&self, symbol: &str, ttl: Duration, now: Instant) -> bool {
        self.action == AiAction::Trade
            && self.symbols.iter().any(|candidate| candidate == symbol)
            && self.at.is_some_and(|at| now.duration_since(at) <= ttl)
    }
}

/// Renders the cleared pairs for the gate log, e.g. `3 pairs: A, B, C`.
fn format_gate_symbols(symbols: &[String]) -> String {
    if symbols.is_empty() {
        return "no watchlist pairs".to_string();
    }
    let names = symbols
        .iter()
        .map(|symbol| symbol.to_uppercase())
        .collect::<Vec<_>>()
        .join(", ");
    let noun = if symbols.len() == 1 { "pair" } else { "pairs" };
    format!("{} {noun}: {names}", symbols.len())
}

/// Resolves a normalised model symbol to the watchlist. Models occasionally
/// drop the quote suffix (`XMR` instead of `XMRUSDT`); accepting that shorthand
/// only when the full pair is actually streamed keeps the gate useful without
/// letting a hallucinated market through.
fn resolve_watchlist_symbol(board: &MarketBoard, candidate: &str) -> Option<String> {
    if board.is_active(candidate) {
        return Some(candidate.to_string());
    }
    let with_quote = format!("{candidate}usdt");
    if board.is_active(&with_quote) {
        Some(with_quote)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Purpose {
    Entry,
    Exit,
}

#[derive(Debug)]
struct TrackedOrder {
    request: OrderRequest,
    report: OrderReport,
    purpose: Purpose,
    created_at: Instant,
    /// Volume already booked into the portfolio, so partial fills are applied
    /// exactly once.
    applied_filled: Decimal,
    /// `true` while the accepted entry is represented by a local pending
    /// position that has not been confirmed by a fill.
    pending_entry: bool,
    /// Failed cancel attempts so far, driving the retry backoff and the halt.
    cancel_attempts: u32,
    /// Earliest instant the next cancel attempt may be sent.
    next_cancel_at: Option<Instant>,
}

/// The state machine.
pub struct Executor {
    cfg: ExecutorConfig,
    broker: Broker,
    portfolio: Portfolio,
    risk: RiskGuard,
    gate: Gate,
    working: Option<TrackedOrder>,
    /// Symbol of the open position (or of the entry order being filled).
    open_symbol: Option<String>,
    /// The shared multi-pair board: signals, prices, steps, watchlist.
    board: Arc<RwLock<MarketBoard>>,
    trades: u32,
    wins: u32,
    open_fees: Decimal,
    opened_at: Option<Instant>,
    /// Highest executable mark (the bid for a long) since the entry filled.
    /// The ATR trailing stop is measured down from here.
    peak_mark: Option<Decimal>,
    exit_retry_after: Option<Instant>,
    last_mark: Option<Decimal>,
    last_status_poll: Instant,
    /// Latest USDT balance of the live venue, mirrored into the AI context.
    live_balance: Option<LiveBalance>,
    last_balance_poll: Instant,
    halt_logged: bool,
    account_tx: watch::Sender<AccountView>,
    /// Scanner used at startup to price and liquidate balances that are not on
    /// the current watchlist (e.g. a coin bought before a restart).
    scanner: Option<Arc<Scanner>>,
    /// Pushes the pinned symbol set to the websocket task after recovery.
    subscriptions_tx: Option<watch::Sender<Vec<String>>>,
}

impl Executor {
    /// Builds an executor for the chosen mode. Live mode fails fast when the
    /// keys are absent instead of silently trading the paper book.
    pub fn new(
        cfg: ExecutorConfig,
        mode: ExecutionMode,
        board: Arc<RwLock<MarketBoard>>,
        account_tx: watch::Sender<AccountView>,
    ) -> Result<Self, ExecutionError> {
        let broker = match mode {
            ExecutionMode::Paper => Broker::paper(),
            ExecutionMode::Live => Broker::live(HtxRest::from_env()?),
            ExecutionMode::Off => {
                return Err(ExecutionError::Config(
                    "execution engine is off; nothing to run".into(),
                ))
            }
        };

        let risk = RiskGuard::new(RiskLimits {
            min_order_notional: cfg.min_order_notional,
            max_order_notional: cfg.max_order_notional,
            max_position_notional: cfg.max_position_notional,
            max_daily_loss: cfg.max_daily_loss,
            max_open_orders: 1,
            cooldown: cfg.cooldown,
        });

        let portfolio = Portfolio::new(match mode {
            ExecutionMode::Paper => cfg.paper_start_cash,
            _ => Decimal::ZERO,
        });

        Ok(Self {
            cfg,
            broker,
            portfolio,
            risk,
            gate: Gate::closed(),
            working: None,
            open_symbol: None,
            board,
            trades: 0,
            wins: 0,
            open_fees: Decimal::ZERO,
            opened_at: None,
            peak_mark: None,
            exit_retry_after: None,
            last_mark: None,
            last_status_poll: Instant::now(),
            live_balance: None,
            last_balance_poll: Instant::now(),
            halt_logged: false,
            account_tx,
            scanner: None,
            subscriptions_tx: None,
        })
    }

    /// Supplies market data used by the startup balance audit. Live mode should
    /// set both the scanner and the subscriptions channel; paper mode can omit
    /// them because it has no venue balances to recover.
    pub fn with_balance_recovery(
        mut self,
        scanner: Arc<Scanner>,
        subscriptions_tx: watch::Sender<Vec<String>>,
    ) -> Self {
        self.scanner = Some(scanner);
        self.subscriptions_tx = Some(subscriptions_tx);
        self
    }

    fn board(&self) -> RwLockReadGuard<'_, MarketBoard> {
        self.board
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn board_mut(&self) -> RwLockWriteGuard<'_, MarketBoard> {
        self.board
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn snapshot_for(&self, symbol: &str) -> Option<MarketSnapshot> {
        self.board().snapshot(symbol)
    }

    fn steps_for(&self, symbol: &str) -> (Decimal, Decimal) {
        self.board()
            .steps(symbol)
            .unwrap_or((self.cfg.base_step, self.cfg.price_step))
    }

    /// Keeps the open position's symbol streaming until it is flat, and pushes
    /// the new pinned set to the websocket task immediately.
    fn pin_board(&self, symbol: Option<String>) {
        let subscriptions = {
            let mut board = self.board_mut();
            board.pin(symbol);
            board.subscriptions()
        };
        if let Some(tx) = &self.subscriptions_tx {
            tx.send_replace(subscriptions);
        }
    }

    /// Runs until a [`ExecutionCommand::Halt`] arrives or the command channel
    /// closes.
    pub async fn run(mut self, mut commands: mpsc::Receiver<ExecutionCommand>) {
        println!(
            "[EXEC] mode={} pairs={} order_notional={}USDT trail={}xATR sl={}% trend_exit={} gate_ttl={}s order_ttl={}s entries={}",
            self.broker.name(),
            self.board().pairs().len(),
            self.cfg.order_notional,
            self.cfg.trail_atr_multiple,
            self.cfg.stop_loss_pct,
            self.cfg.trend_exit,
            self.cfg.gate_ttl.as_secs(),
            self.cfg.order_ttl.as_secs(),
            if self.cfg.breakout_taker {
                "breakout:taker pullback:maker"
            } else {
                "breakout:maker pullback:maker"
            },
        );
        if !self.broker.is_paper() {
            self.live_preflight().await;
        }
        self.publish_account();

        let mut ticker = interval(TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                command = commands.recv() => {
                    let Some(command) = command else { break };
                    if !self.handle_command(command).await {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    self.refresh_live_balance().await;
                    self.on_tick().await;
                }
            }
        }

        self.report("final");
    }

    /// Handles one command. Returns `false` when the executor should stop.
    async fn handle_command(&mut self, command: ExecutionCommand) -> bool {
        match command {
            ExecutionCommand::Signal(signal) => {
                self.try_enter(signal).await;
                true
            }
            ExecutionCommand::Verdict {
                action,
                symbols,
                reason,
            } => {
                let mut seen = HashSet::new();
                let requested: Vec<String> = symbols
                    .into_iter()
                    .filter_map(|symbol| crate::scanner::normalize_symbol(&symbol))
                    .filter(|symbol| seen.insert(symbol.clone()))
                    .collect();
                let requested_empty = requested.is_empty();

                let valid_symbols: Vec<String> = match action {
                    AiAction::Trade => {
                        let mut resolved_seen = HashSet::new();
                        requested
                            .into_iter()
                            .filter_map(|candidate| {
                                match resolve_watchlist_symbol(&self.board(), &candidate) {
                                    Some(symbol) if resolved_seen.insert(symbol.clone()) => {
                                        Some(symbol)
                                    }
                                    Some(_) => None,
                                    None => {
                                        println!(
                                            "[EXEC] AI picked {}, which is not on the scanner watchlist; ignoring it",
                                            candidate.to_uppercase()
                                        );
                                        None
                                    }
                                }
                            })
                            .collect()
                    }
                    AiAction::Pause => Vec::new(),
                };

                if action == AiAction::Trade {
                    if requested_empty {
                        println!("[EXEC] AI sent TRADE without any symbol; gate stays closed");
                    }
                    println!(
                        "[EXEC] AI gate TRADE {} — {}",
                        format_gate_symbols(&valid_symbols),
                        reason
                    );
                    if valid_symbols.is_empty() && !requested_empty {
                        println!(
                            "[EXEC] gate stays closed: none of the AI pairs is on the scanner watchlist"
                        );
                    }
                } else {
                    println!("[EXEC] AI gate PAUSE — {reason}");
                }
                if action == AiAction::Pause && !self.portfolio.position.is_flat() {
                    println!(
                        "[EXEC] entries disabled by the AI; the open position keeps its TP/SL"
                    );
                }
                self.gate = Gate {
                    action,
                    symbols: valid_symbols,
                    reason,
                    at: Some(Instant::now()),
                };
                true
            }
            ExecutionCommand::Halt(reason) => {
                self.handle_halt(reason).await;
                false
            }
        }
    }

    /// A dislocation arrived: gate, size, risk-check, place.
    async fn try_enter(&mut self, signal: Signal) {
        let symbol = signal.symbol.clone();

        // Spot is long-only. A SELL is only ever produced by the exit path
        // (`maybe_exit` / kill switch) for a position that is already open; it
        // must never be interpreted as an entry signal, even if one slips
        // through from an older strategy build.
        if signal.side != Side::Buy {
            println!(
                "[EXEC] skip {} {}: spot is long-only; SELL only closes an open position",
                symbol.to_uppercase(),
                signal.side.as_str()
            );
            return;
        }

        if !self.board().is_active(&symbol) {
            println!(
                "[EXEC] skip {} {}: not on the scanner watchlist",
                symbol.to_uppercase(),
                signal.side.as_str()
            );
            return;
        }
        let Some(snapshot) = self.snapshot_for(&symbol) else {
            println!(
                "[EXEC] skip {} {}: no market data yet",
                symbol.to_uppercase(),
                signal.side.as_str()
            );
            return;
        };
        if !self.portfolio.position.is_flat() {
            println!(
                "[EXEC] skip {} {}: position already open ({} {})",
                symbol.to_uppercase(),
                signal.side.as_str(),
                self.portfolio.position.qty(),
                self.open_symbol.as_deref().unwrap_or("?").to_uppercase()
            );
            return;
        }
        if self.working.is_some() {
            println!(
                "[EXEC] skip {} {}: an order is already working",
                symbol.to_uppercase(),
                signal.side.as_str()
            );
            return;
        }

        let now = Instant::now();
        if !self.gate.permits(&symbol, self.cfg.gate_ttl, now) {
            println!(
                "[EXEC] skip {} {}: AI gate is {} for {} ({})",
                symbol.to_uppercase(),
                signal.side.as_str(),
                self.gate.action.as_str(),
                format_gate_symbols(&self.gate.symbols),
                self.gate.reason
            );
            return;
        }

        let (base_step, price_step) = self.steps_for(&symbol);
        // A breakout is momentum: by the time price clears the Donchian level the
        // bid it would rest on is already behind the move, so those entries take
        // liquidity. A pullback is bought *into* a dip, where resting just above
        // the bid earns the spread instead of paying it.
        let kind = match (signal.kind, self.cfg.breakout_taker) {
            (EntryKind::Breakout, true) => OrderKind::Market,
            _ => OrderKind::MakerLimit,
        };
        let limit = match kind {
            OrderKind::Market => entry_price(signal.side, &snapshot).unwrap_or_default(),
            OrderKind::MakerLimit => {
                match maker_price(
                    signal.side,
                    &snapshot,
                    price_step,
                    self.cfg.maker_offset_ticks,
                ) {
                    Some(limit) => limit,
                    None => {
                        println!(
                            "[EXEC] skip {} {}: no non-crossing top of book for a maker entry",
                            symbol.to_uppercase(),
                            signal.side.as_str()
                        );
                        return;
                    }
                }
            }
        };
        if limit <= Decimal::ZERO {
            println!(
                "[EXEC] skip {} {}: no price to size the entry against",
                symbol.to_uppercase(),
                signal.side.as_str()
            );
            return;
        }
        let target_notional = self.entry_notional(&symbol);
        // Size against the limit, not the touch: the fee must fit the balance
        // and a maker order must never spend more than the configured notional.
        let amount = size_order(target_notional, limit, base_step);
        if amount <= Decimal::ZERO {
            println!(
                "[EXEC] skip {}: {} USDT buys less than one {}-step at {}",
                symbol.to_uppercase(),
                target_notional,
                base_step,
                limit
            );
            return;
        }

        let mark = snapshot.mark_price().unwrap_or(limit);
        let request = OrderRequest {
            symbol: symbol.clone(),
            side: signal.side,
            amount,
            price: limit,
            kind,
            client_order_id: self.next_client_id(),
        };

        let position_notional = self.portfolio.position.notional(mark);
        let cash = self.available_cash();
        if let Err(rejection) = self.risk.check(
            Intent::Entry,
            &request,
            mark,
            position_notional,
            usize::from(self.working.is_some()),
            cash,
            now,
        ) {
            println!("[EXEC] risk rejected {}: {rejection}", signal.side.as_str());
            return;
        }

        if let Some(cash) = cash {
            let notional = request.notional();
            let fee = notional * self.cfg.fee_rate();
            let affordable = if self.broker.is_paper() {
                self.portfolio.can_afford(notional, fee)
            } else {
                cash >= notional + fee
            };
            if !affordable {
                println!(
                    "[EXEC] risk rejected {}: need {} + fee {}, have {}",
                    signal.side.as_str(),
                    notional,
                    fee,
                    cash
                );
                return;
            }
        }

        println!(
            "[EXEC] placing {} {} {} {} @ {} (signal: {} {})",
            kind.as_str(),
            signal.side.as_str(),
            amount,
            symbol.to_uppercase(),
            limit,
            signal.kind.as_str(),
            signal.reason
        );

        match self.broker.place(&request, &snapshot).await {
            Ok(report) => {
                println!(
                    "[EXEC] order {} accepted: {}",
                    report.order_id,
                    report.status.as_str()
                );
                self.open_symbol = Some(symbol.clone());
                self.pin_board(Some(symbol));
                self.working = Some(TrackedOrder {
                    request,
                    report,
                    purpose: Purpose::Entry,
                    created_at: now,
                    applied_filled: Decimal::ZERO,
                    pending_entry: false,
                    cancel_attempts: 0,
                    next_cancel_at: None,
                });
                self.absorb_working().await;
            }
            Err(error) => eprintln!("[EXEC] placement failed: {error}"),
        }
    }

    /// Books any new fill of the working order into the portfolio exactly once,
    /// then retires the order when the venue reports a terminal state.
    async fn absorb_working(&mut self) {
        let Some(mut working) = self.working.take() else {
            return;
        };

        let delta = working.report.filled_amount - working.applied_filled;
        if delta > Decimal::ZERO {
            // A pending overlay only describes an unfilled order. Remove it
            // before booking the real fill so the quantity is never counted
            // twice (and cash moves exactly once).
            if working.pending_entry {
                self.portfolio.clear_pending_entry();
                working.pending_entry = false;
            }
            let price = working.report.avg_price().unwrap_or(working.request.price);
            let fee = delta * price * self.cfg.fee_rate();
            let realized = self
                .portfolio
                .apply_fill(working.request.side, delta, price);
            self.portfolio.charge_fee(fee);
            self.risk.record_fill(realized - fee);
            working.applied_filled += delta;

            match working.purpose {
                Purpose::Entry => {
                    self.open_fees += fee;
                    if self.opened_at.is_none() {
                        self.opened_at = Some(Instant::now());
                        self.peak_mark = Some(price);
                    }
                    if self.open_symbol.as_deref() != Some(working.request.symbol.as_str()) {
                        self.open_symbol = Some(working.request.symbol.clone());
                        self.pin_board(self.open_symbol.clone());
                    }
                    println!(
                        "[EXEC] entry fill {} {} @ {} (fee {})",
                        delta,
                        working.request.symbol.to_uppercase(),
                        price.round_dp(8),
                        fee.round_dp(8)
                    );
                }
                Purpose::Exit => {
                    println!(
                        "[EXEC] exit fill {} {} @ {} (fee {})",
                        delta,
                        working.request.symbol.to_uppercase(),
                        price.round_dp(8),
                        fee.round_dp(8)
                    );
                    if self.portfolio.position.is_flat() {
                        self.settle_trade(realized - fee);
                    }
                }
            }
        }

        // The venue accepted the entry but has not filled it yet. Record it in
        // the local portfolio immediately: the risk guard must see the exposure
        // and a second signal must not look like a flat-book entry.
        if working.purpose == Purpose::Entry
            && !working.report.status.is_terminal()
            && !working.pending_entry
        {
            let remaining = working.request.amount - working.applied_filled;
            if self.portfolio.begin_pending_entry(
                remaining,
                working.request.price,
                self.broker.is_paper(),
            ) {
                working.pending_entry = true;
                println!(
                    "[EXEC] portfolio pending BUY {} {} @ {} (order {})",
                    remaining,
                    working.request.symbol.to_uppercase(),
                    working.request.price,
                    working.report.order_id
                );
            }
        }

        if delta > Decimal::ZERO && !self.broker.is_paper() {
            // The venue balance is authoritative right after a fill: keep the
            // AI context in sync without waiting for the next 30s poll.
            let previous = self.live_balance;
            match self.sync_live_balance().await {
                Ok(balance) => log_live_balance(previous, balance),
                Err(error) => eprintln!("[EXEC] balance refresh after fill failed: {error}"),
            }
        }

        if working.report.status.is_terminal() {
            if working.pending_entry {
                // Cancelled or rejected before filling: the optimistic entry
                // never became a position.
                self.portfolio.clear_pending_entry();
                working.pending_entry = false;
            }
            if working.purpose == Purpose::Entry
                && working.report.status != OrderStatus::Filled
                && working.applied_filled > Decimal::ZERO
            {
                println!(
                    "[EXEC] entry order {} ended {} with a partial fill of {}",
                    working.report.order_id,
                    working.report.status.as_str(),
                    working.applied_filled
                );
            }
            if working.purpose == Purpose::Exit && !self.portfolio.position.is_flat() {
                self.exit_retry_after = Some(Instant::now() + EXIT_RETRY_DELAY);
                println!(
                    "[EXEC] exit order {} ended {} but the position is still open; retrying in {}s",
                    working.report.order_id,
                    working.report.status.as_str(),
                    EXIT_RETRY_DELAY.as_secs()
                );
            }
        } else {
            self.working = Some(working);
        }
    }

    fn settle_trade(&mut self, exit_realized: Decimal) {
        let net = exit_realized - self.open_fees;
        self.trades += 1;
        if net > Decimal::ZERO {
            self.wins += 1;
        }
        println!(
            "[EXEC] trade #{} closed: net {} USDT ({} of {} winners)",
            self.trades,
            net.round_dp(4),
            self.wins,
            self.trades
        );
        self.open_fees = Decimal::ZERO;
        self.opened_at = None;
        self.peak_mark = None;
        self.exit_retry_after = None;
        self.open_symbol = None;
        self.pin_board(None);
    }

    /// Cancels a tracked order and books anything that filled before the
    /// cancel landed. Returns `true` when the order is no longer live.
    ///
    /// HTX reports cancelling an already-terminal order as an API error, so an
    /// error is not proof that the order is still resting. The status is
    /// queried before deciding: a filled order is absorbed normally, an order
    /// the venue does not know about is treated as gone, and only an order that
    /// is demonstrably still live stays tracked — with a growing backoff, so a
    /// cancel that keeps failing cannot become a request loop. Never fake a
    /// cancellation: that would leave a real order (and its pending position)
    /// unmanaged.
    async fn cancel_and_absorb(&mut self, mut working: TrackedOrder, label: &str) -> bool {
        match self.broker.cancel(&working.report).await {
            Ok(()) => {
                println!(
                    "[EXEC] {label}: cancelled order {}",
                    working.report.order_id
                );
                self.book_final_fill(working, label).await;
                true
            }
            Err(error) => {
                eprintln!(
                    "[EXEC] {label}: cancel failed for {}: {error}",
                    working.report.order_id
                );
                let vanished = is_order_not_found(&error);
                match self.broker.status(&working.report).await {
                    Ok(update) if update.status.is_terminal() => {
                        working.report = update;
                        self.working = Some(working);
                        self.absorb_working().await;
                        true
                    }
                    Ok(update) => {
                        eprintln!(
                            "[EXEC] {label}: order {} is still {}; keeping it tracked",
                            update.order_id,
                            update.status.as_str()
                        );
                        working.report = update;
                        self.defer_cancel(&mut working, label);
                        self.working = Some(working);
                        self.absorb_working().await;
                        false
                    }
                    Err(status_error) if vanished && is_order_not_found(&status_error) => {
                        // The venue knows neither how to cancel it nor how to
                        // find it: the order is gone, so waiting longer can
                        // only keep the local book hostage.
                        eprintln!(
                            "[EXEC] {label}: order {} is unknown to the venue ({status_error}); treating it as ended",
                            working.report.order_id
                        );
                        working.report.status = OrderStatus::Canceled;
                        self.working = Some(working);
                        self.absorb_working().await;
                        true
                    }
                    Err(status_error) => {
                        eprintln!(
                            "[EXEC] {label}: could not confirm {} after a failed cancel: {status_error}",
                            working.report.order_id
                        );
                        self.defer_cancel(&mut working, label);
                        self.working = Some(working);
                        false
                    }
                }
            }
        }
    }

    /// Retires a cancelled order after one last look at its fill.
    ///
    /// A cancel response says nothing about units that filled between the last
    /// status poll and the cancel itself. One extra query makes the reported
    /// fill authoritative instead of assumed; without it those units would sit
    /// on the venue balance with no exit managing them.
    async fn book_final_fill(&mut self, mut working: TrackedOrder, label: &str) {
        working.report.status = OrderStatus::Canceled;
        if !self.broker.is_paper() {
            match self.broker.status(&working.report).await {
                Ok(update) => {
                    let reported = update.status;
                    working.report = update;
                    if !reported.is_terminal() {
                        // The cancel was accepted; a state that has not advanced
                        // yet must not keep the order tracked forever.
                        working.report.status = OrderStatus::Canceled;
                        eprintln!(
                            "[EXEC] {label}: order {} was cancelled but the venue still reports {}; treating it as closed",
                            working.report.order_id,
                            reported.as_str()
                        );
                    }
                }
                Err(error) => eprintln!(
                    "[EXEC] {label}: final status for {} unavailable after the cancel: {error}",
                    working.report.order_id
                ),
            }
        }
        self.working = Some(working);
        self.absorb_working().await;
    }

    /// Schedules the next cancel attempt and halts entries once the venue keeps
    /// refusing. An order that cannot be cancelled is unmanaged exposure and
    /// has to be loud, not a silent retry loop.
    fn defer_cancel(&mut self, working: &mut TrackedOrder, label: &str) {
        working.cancel_attempts = working.cancel_attempts.saturating_add(1);
        let backoff = cancel_backoff(working.cancel_attempts);
        working.next_cancel_at = Some(Instant::now() + backoff);
        eprintln!(
            "[EXEC] {label}: cancel attempt {} for {} failed; retrying in {}s",
            working.cancel_attempts,
            working.report.order_id,
            backoff.as_secs()
        );
        if working.cancel_attempts >= CANCEL_ATTEMPT_LIMIT {
            let reason = format!(
                "order {} cannot be cancelled after {} attempts",
                working.report.order_id, working.cancel_attempts
            );
            eprintln!("[EXEC] {label}: HALTING entries — {reason}; cancel it by hand on HTX");
            self.risk.halt(reason);
        }
    }

    /// `true` when the market has run away from a resting entry.
    ///
    /// A post-only entry is a bet that price comes back to the order. Once the
    /// mark is more than `entry_max_chase_bps` above the limit that bet is lost:
    /// the order can only fill at a price the signal no longer describes, and
    /// waiting out the TTL just delays the next signal.
    fn entry_is_stale(&self, snapshot: &MarketSnapshot) -> bool {
        if self.cfg.entry_max_chase_bps <= Decimal::ZERO {
            return false;
        }
        let Some(working) = self.working.as_ref() else {
            return false;
        };
        if working.purpose != Purpose::Entry
            || working.request.side != Side::Buy
            || working.request.kind != OrderKind::MakerLimit
            || working.report.status.is_terminal()
        {
            return false;
        }
        entry_chase_bps(working, snapshot).is_some_and(|bps| bps > self.cfg.entry_max_chase_bps)
    }

    /// Raises the tracked peak with the executable exit mark.
    ///
    /// The peak only ever moves up, so the trailing stop is measured from the
    /// best price the position could actually have been sold at. The ATR of the
    /// symbol sets the distance, so the stop breathes with volatility instead of
    /// sitting at a fixed percentage.
    fn refresh_peak(&mut self, snapshot: &MarketSnapshot) -> Option<Decimal> {
        if self.working.is_some() || self.portfolio.position.is_flat() || self.opened_at.is_none() {
            return self.peak_mark;
        }
        let qty = self.portfolio.position.qty();
        let avg = self.portfolio.position.avg_entry();
        if avg <= Decimal::ZERO {
            return self.peak_mark;
        }
        let Some(mark) = exit_mark(qty, snapshot) else {
            return self.peak_mark;
        };
        let previous = self.peak_mark;
        let peak = previous.map_or(mark, |peak| peak.max(mark));
        self.peak_mark = Some(peak);

        if previous.is_none_or(|previous| peak > previous) {
            let policy = ExitPolicy::from_config(&self.cfg);
            let floor = policy.trail_floor(peak, snapshot.atr);
            let peak_gain = (peak - avg) / avg * Decimal::from(100);
            let floor_gain = (floor - avg) / avg * Decimal::from(100);
            println!(
                "[EXEC] TRAIL PEAK {} peak={} (+{}%) stop={} (+{}%) atr={}",
                snapshot.symbol.to_uppercase(),
                peak.round_dp(8),
                peak_gain.round_dp(3),
                floor.round_dp(8),
                floor_gain.round_dp(3),
                snapshot
                    .atr
                    .map(|atr| atr.round_dp(8).to_string())
                    .unwrap_or_else(|| "unwarmed".to_string()),
            );
        }
        Some(peak)
    }

    /// Periodic maintenance: fills, live status, order expiry, exits.
    async fn on_tick(&mut self) {
        self.risk.roll_day(Utc::now().date_naive());

        let tracked_symbol = self
            .working
            .as_ref()
            .map(|working| working.request.symbol.clone())
            .or_else(|| self.open_symbol.clone());
        let Some(snapshot) = tracked_symbol
            .as_deref()
            .and_then(|symbol| self.snapshot_for(symbol))
        else {
            // Nothing is held or resting: no fills or exits to manage yet.
            self.publish_account();
            return;
        };

        if Some(snapshot.symbol.as_str()) == self.open_symbol.as_deref() {
            if let Some(mark) = snapshot.mark_price() {
                self.last_mark = Some(mark);
            }
        }

        // Paper fills happen on market moves; live fills arrive via polling.
        let updates = self.broker.on_market(&snapshot);
        if let Some(working) = self.working.as_mut() {
            if let Some(update) = updates
                .iter()
                .find(|update| update.order_id == working.report.order_id)
            {
                working.report = update.clone();
            }
        }

        if !self.broker.is_paper()
            && self.working.is_some()
            && self.last_status_poll.elapsed() >= LIVE_STATUS_POLL
        {
            self.last_status_poll = Instant::now();
            let report = self.working.as_ref().map(|working| working.report.clone());
            if let Some(report) = report {
                if !report.status.is_terminal() {
                    match self.broker.status(&report).await {
                        Ok(update) => {
                            if let Some(working) = self.working.as_mut() {
                                working.report = update;
                            }
                        }
                        Err(error) => {
                            eprintln!("[EXEC] status poll failed for {}: {error}", report.order_id)
                        }
                    }
                }
            }
        }

        self.absorb_working().await;
        self.refresh_peak(&snapshot);

        // A working order that never filled must not camp on the book forever —
        // only entries still rest; exits are market orders now. A cancel that
        // the venue refused is retried on a growing backoff instead of on every
        // tick, and that backoff covers every reason to cancel: a stale entry
        // must not hammer a venue that already refused the cancel.
        let now = Instant::now();
        let ready = self
            .working
            .as_ref()
            .is_none_or(|working| working.next_cancel_at.is_none_or(|after| now >= after));
        let stale = ready && self.entry_is_stale(&snapshot);
        let expired = !stale
            && ready
            && self.working.as_ref().is_some_and(|working| {
                !working.report.status.is_terminal()
                    && working.created_at.elapsed() >= self.cfg.order_ttl
            });
        if stale || expired {
            if let Some(working) = self.working.take() {
                let label = if stale {
                    let chase_bps = entry_chase_bps(&working, &snapshot).unwrap_or_default();
                    println!(
                        "[EXEC] entry invalidation: {} is {}bps above the resting limit {}; cancelling order {}",
                        snapshot.symbol.to_uppercase(),
                        chase_bps.round_dp(1),
                        working.request.price,
                        working.report.order_id
                    );
                    "entry invalidation"
                } else {
                    println!(
                        "[EXEC] cancelling order {} after {}s without a fill",
                        working.report.order_id,
                        self.cfg.order_ttl.as_secs()
                    );
                    "order expiry"
                };
                self.cancel_and_absorb(working, label).await;
            }
        }

        self.maybe_exit().await;

        // An entry that was cancelled or rejected leaves no position and no
        // working order: release the position pin and its book.
        if self.portfolio.position.is_flat() && self.working.is_none() && self.open_symbol.is_some()
        {
            self.open_symbol = None;
            self.pin_board(None);
        }

        if self.risk.is_halted() && !self.halt_logged {
            println!(
                "[EXEC] entries halted: {}",
                self.risk.halt_reason().unwrap_or("unknown reason")
            );
            self.halt_logged = true;
        }

        self.publish_account();
    }

    /// Forgets a residual position the venue refuses to trade.
    ///
    /// The local book becomes flat so `maybe_exit` stops re-placing an order
    /// that can never succeed. No fill happened, so cash, fee and trade counters
    /// are intentionally left untouched.
    fn drop_tracked_position(&mut self) {
        self.portfolio.forget_position();
        self.working = None;
        self.open_symbol = None;
        self.opened_at = None;
        self.peak_mark = None;
        self.exit_retry_after = None;
        self.open_fees = Decimal::ZERO;
        self.last_mark = None;
        self.pin_board(None);
        self.publish_account();
    }

    /// Closes the position on the trailing stop or the hard stop-loss, using the
    /// book of the symbol that is actually held.
    async fn maybe_exit(&mut self) {
        if self.working.is_some() || self.portfolio.position.is_flat() {
            return;
        }

        let Some(symbol) = self.open_symbol.clone() else {
            eprintln!("[EXEC] open position has no symbol; cannot choose an exit market");
            return;
        };
        let Some(snapshot) = self.snapshot_for(&symbol) else {
            eprintln!(
                "[EXEC] no market data for {}; holding the position and waiting",
                symbol.to_uppercase()
            );
            return;
        };

        let now = Instant::now();
        if self.exit_retry_after.is_some_and(|after| now < after) {
            return;
        }

        let position = self.portfolio.position.clone();
        let qty = position.qty();
        let avg = position.avg_entry();
        if avg <= Decimal::ZERO {
            return;
        }

        let Some(mark) = exit_mark(qty, &snapshot) else {
            return;
        };
        // A missing peak (e.g. a position adopted without a fill) still gets the
        // hard stop: fall back to the current mark so the peak is the mark.
        let peak = self.refresh_peak(&snapshot).unwrap_or(mark);
        let pnl_pct = (mark - avg) / avg * Decimal::from(100);
        let peak_gain_pct = (peak - avg) / avg * Decimal::from(100);
        let policy = ExitPolicy::from_config(&self.cfg);
        let atr = snapshot.atr;

        let Some(reason) = exit_decision(mark, peak, avg, atr, snapshot.trend_up, &policy) else {
            return;
        };

        let side = if qty > Decimal::ZERO {
            Side::Sell
        } else {
            Side::Buy
        };
        let amount = qty.abs();
        let (_, price_step) = self.steps_for(&symbol);
        // Both exits are unconditional: a trailing stop that cannot fill would
        // just be a delayed stop-loss, so it hits the market like the hard stop.
        let kind = OrderKind::Market;
        let limit = aggressive_limit(self.cfg.slippage_bps, side, mark, price_step);
        match reason {
            ExitReason::TrailingStop => println!(
                "[EXEC] ATR trailing stop hit {}: peak={} (+{}%) stop={} atr={} mark={} (+{}%)",
                symbol.to_uppercase(),
                peak.round_dp(8),
                peak_gain_pct.round_dp(3),
                policy.trail_floor(peak, atr).round_dp(8),
                atr.map(|atr| atr.round_dp(8).to_string())
                    .unwrap_or_else(|| "unwarmed".to_string()),
                mark.round_dp(8),
                pnl_pct.round_dp(3)
            ),
            ExitReason::TrendFlip => println!(
                "[EXEC] trend flip exit {}: mark={} pnl={}% adx={} ema_fast={} ema_slow={} ema_trend={}",
                symbol.to_uppercase(),
                mark.round_dp(8),
                pnl_pct.round_dp(3),
                opt(snapshot.adx, 1),
                opt(snapshot.ema_fast, 8),
                opt(snapshot.ema_slow, 8),
                opt(snapshot.ema_trend, 8),
            ),
            ExitReason::StopLoss => println!(
                "[EXEC] hard stop-loss hit {}: mark={} pnl={}% limit={}%",
                symbol.to_uppercase(),
                mark.round_dp(8),
                pnl_pct.round_dp(3),
                -self.cfg.stop_loss_pct
            ),
        }
        let request = OrderRequest {
            symbol: symbol.clone(),
            side,
            amount,
            price: limit,
            kind,
            client_order_id: self.next_client_id(),
        };

        if let Err(rejection) = self.risk.check(
            Intent::Exit,
            &request,
            mark,
            position.notional(mark),
            0,
            None,
            now,
        ) {
            println!("[EXEC] exit rejected by risk: {rejection}");
            return;
        }

        println!(
            "[EXEC] placing {} {} {} {} @ {} — {} (pnl {}%)",
            kind.as_str(),
            side.as_str(),
            amount,
            symbol.to_uppercase(),
            limit,
            reason.as_str(),
            pnl_pct.round_dp(3)
        );

        match self.broker.place(&request, &snapshot).await {
            Ok(report) => {
                self.working = Some(TrackedOrder {
                    request,
                    report,
                    purpose: Purpose::Exit,
                    created_at: now,
                    applied_filled: Decimal::ZERO,
                    pending_entry: false,
                    cancel_attempts: 0,
                    next_cancel_at: None,
                });
                self.absorb_working().await;
            }
            Err(error) => {
                if is_order_value_min_error(&error) {
                    eprintln!(
                        "[EXEC] exit rejected as below the venue minimum: {error}; dropping the position so it is not retried"
                    );
                    self.drop_tracked_position();
                    return;
                }
                self.exit_retry_after = Some(now + EXIT_RETRY_DELAY);
                eprintln!("[EXEC] exit placement failed: {error}");
            }
        }
    }

    /// Operator kill switch: latch the guard, cancel working orders and flatten.
    async fn handle_halt(&mut self, reason: String) {
        println!("[EXEC] HALT: {reason}");
        self.risk.halt(reason);

        if let Some(working) = self.working.take() {
            let settled = self.cancel_and_absorb(working, "halt").await;
            if !settled {
                eprintln!(
                    "[EXEC] HALT: a working order is still live on the venue; refusing to place a second exit order until it is resolved"
                );
                self.publish_account();
                return;
            }
        }

        if !self.portfolio.position.is_flat() {
            let Some(symbol) = self.open_symbol.clone() else {
                eprintln!("[EXEC] kill switch: the position has no symbol; cannot flatten");
                self.publish_account();
                return;
            };
            let snapshot = self.snapshot_for(&symbol).unwrap_or_default();
            let position = self.portfolio.position.clone();
            let qty = position.qty();
            let side = if qty > Decimal::ZERO {
                Side::Sell
            } else {
                Side::Buy
            };
            let (_, price_step) = self.steps_for(&symbol);
            let touch = entry_price(side, &snapshot).or(self.last_mark);

            if let Some(touch) = touch {
                let limit = aggressive_limit(self.cfg.slippage_bps, side, touch, price_step);
                let request = OrderRequest {
                    symbol: symbol.clone(),
                    side,
                    amount: qty.abs(),
                    price: limit,
                    kind: OrderKind::Market,
                    client_order_id: self.next_client_id(),
                };
                match self.broker.place(&request, &snapshot).await {
                    Ok(report) => {
                        println!(
                            "[EXEC] kill switch: {} {} {} @ {} → order {} [{}]",
                            side.as_str(),
                            qty.abs(),
                            symbol.to_uppercase(),
                            limit,
                            report.order_id,
                            report.status.as_str()
                        );
                        self.working = Some(TrackedOrder {
                            request,
                            report,
                            purpose: Purpose::Exit,
                            created_at: Instant::now(),
                            applied_filled: Decimal::ZERO,
                            pending_entry: false,
                            cancel_attempts: 0,
                            next_cancel_at: None,
                        });
                        self.absorb_working().await;
                    }
                    Err(error) => eprintln!(
                        "[EXEC] kill-switch order failed: {error}; the position is still open"
                    ),
                }
            } else {
                eprintln!("[EXEC] kill switch: no price available to flatten the position");
            }
        }

        self.open_symbol = None;
        self.pin_board(None);
        self.publish_account();
    }

    /// Reads the live balances at boot, recovers any held coin into a managed
    /// position and liquidates leftover balances, before the first signal.
    async fn live_preflight(&mut self) {
        self.last_balance_poll = Instant::now();
        let balances = match self.broker.balances().await {
            Ok(balances) => balances,
            Err(error) => {
                eprintln!("[EXEC] could not load balances: {error}");
                return;
            }
        };
        let balance = self.remember_live_balance(&balances);
        log_balance_audit(&balances);
        println!("[EXEC] live balances: USDT={}", balance.available);
        if balance.available < self.cfg.order_notional {
            eprintln!(
                "[EXEC] WARNING: USDT balance {} is below the {} order notional; entries will be capped to the balance",
                balance.available, self.cfg.order_notional
            );
        }

        self.recover_live_positions(&balances).await;
    }

    /// Mirrors the USDT row into the AI context. A missing row means zero.
    fn remember_live_balance(&mut self, balances: &[Balance]) -> LiveBalance {
        let balance = usdt_balance(balances).unwrap_or_default();
        self.live_balance = Some(balance);
        balance
    }

    /// Startup portfolio audit.
    ///
    /// The local portfolio is not persisted, so after a restart the venue is
    /// the only source of truth. The largest non-USDT balance with a tradable
    /// USDT market is adopted as the open position at the current mark, with
    /// TP/SL armed around it. Every other balance is sold into USDT at market.
    /// A balance worth less than the venue minimum order is logged as dust and
    /// skipped: it cannot be sold. A balance without a USDT market cannot be
    /// priced or sold and is reported and left untouched rather than guessed at.
    async fn recover_live_positions(&mut self, balances: &[Balance]) {
        if !self.portfolio.position.is_flat() {
            println!(
                "[EXEC] recovery skipped: the executor already tracks a {} position",
                self.open_symbol.as_deref().unwrap_or("?").to_uppercase()
            );
            return;
        }

        let Some(scanner) = self.scanner.clone() else {
            eprintln!(
                "[EXEC] recovery unavailable: no scanner was wired into the executor; non-USDT balances stay untouched"
            );
            return;
        };
        let quotes = match scanner.quotes().await {
            Ok(quotes) => quotes,
            Err(error) => {
                eprintln!(
                    "[EXEC] recovery could not load spot quotes ({error}); non-USDT balances stay untouched"
                );
                return;
            }
        };

        let found = recovery_holdings(balances, &quotes, self.cfg.base_step, self.cfg.price_step);
        if found.is_empty() {
            println!("[EXEC] recovery: no non-USDT balances");
            return;
        }

        let mut holdings = Vec::with_capacity(found.len());
        for holding in found {
            if is_dust_holding(&holding) {
                println!(
                    "[EXEC] ignoring dust: {} qty={} value={}",
                    holding.currency,
                    holding.qty,
                    holding.notional().round_dp(8)
                );
                continue;
            }
            match holding.mark {
                Some(mark) => println!(
                    "[EXEC] recovery: found {} qty={} free={} mark={} value={}USDT",
                    holding.currency,
                    holding.qty,
                    holding.available,
                    mark,
                    holding.notional().round_dp(4)
                ),
                None => eprintln!(
                    "[EXEC] recovery: {} qty={} has no USDT market; it cannot be priced or sold and is left untouched",
                    holding.currency, holding.qty
                ),
            }
            holdings.push(holding);
        }
        if holdings.is_empty() {
            println!("[EXEC] recovery: nothing tradable left after dust filtering");
        }

        let mut primary = pick_primary_holding(&holdings);
        if let Some(index) = primary {
            if !self.adopt_recovered_position(&holdings[index]) {
                eprintln!(
                    "[EXEC] recovery: could not adopt {}; it will be liquidated instead",
                    holdings[index].currency
                );
                primary = None;
            }
        } else if !holdings.is_empty() {
            eprintln!(
                "[EXEC] recovery: no balance has a tradable USDT market to manage as a position"
            );
        }

        for (index, holding) in holdings.iter().enumerate() {
            if Some(index) == primary || holding.mark.is_none() {
                continue;
            }
            self.liquidate_recovered_holding(holding).await;
        }

        let previous = self.live_balance;
        match self.sync_live_balance().await {
            Ok(balance) => log_live_balance(previous, balance),
            Err(error) => eprintln!("[EXEC] balance refresh after recovery failed: {error}"),
        }
    }

    /// Restores one recovered balance as the open long position.
    ///
    /// The entry basis is the mark at restart: the exchange balance does not
    /// carry the original cost basis and the bot does not persist fills. TP/SL
    /// are therefore anchored to the recovery price, which still guarantees the
    /// position is protected from this point onward.
    fn adopt_recovered_position(&mut self, holding: &RecoveredHolding) -> bool {
        let Some(mark) = holding.mark else {
            return false;
        };
        let rounded = round_to_step(holding.qty, holding.base_step, false);
        let qty = if rounded > Decimal::ZERO {
            rounded
        } else {
            holding.qty
        };
        if !self.portfolio.restore_long(qty, mark) {
            return false;
        }

        let symbol = holding.symbol.clone();
        self.open_symbol = Some(symbol.clone());
        self.pin_board(Some(symbol.clone()));
        self.board_mut()
            .set_steps(&symbol, holding.base_step, holding.price_step);
        self.last_mark = Some(mark);
        self.opened_at = Some(Instant::now());
        self.peak_mark = Some(mark);
        self.exit_retry_after = None;
        self.open_fees = Decimal::ZERO;
        self.working = None;

        let stop_loss = mark * (Decimal::ONE - self.cfg.stop_loss_pct / Decimal::from(100));
        println!(
            "[EXEC] recovered position: {} qty={} entry={} (restart mark) trail={}xATR sl={}",
            symbol.to_uppercase(),
            qty,
            mark.round_dp(8),
            self.cfg.trail_atr_multiple,
            stop_loss.round_dp(8)
        );
        if qty != holding.qty {
            println!(
                "[EXEC] recovered position rounded {} down to the venue step {}: unmanaged remainder {}",
                holding.qty,
                holding.base_step,
                holding.qty - qty
            );
        }
        if holding.qty > holding.available {
            eprintln!(
                "[EXEC] WARNING: {} of {} is frozen; exits may be rejected until the venue releases it",
                holding.qty - holding.available,
                symbol.to_uppercase()
            );
        }
        true
    }

    /// Market-sells a non-primary balance back into USDT.
    async fn liquidate_recovered_holding(&mut self, holding: &RecoveredHolding) {
        if holding.available <= Decimal::ZERO {
            eprintln!(
                "[EXEC] recovery: {} has no free balance to sell (frozen {})",
                holding.currency, holding.qty
            );
            return;
        }
        let amount = round_to_step(holding.available, holding.base_step, false);
        if amount <= Decimal::ZERO {
            eprintln!(
                "[EXEC] recovery: {} balance {} is below the venue step {}; cannot sell it",
                holding.currency, holding.available, holding.base_step
            );
            return;
        }
        let Some(mark) = holding.mark else {
            return;
        };
        let Some(touch) = holding
            .best_bid
            .filter(|bid| *bid > Decimal::ZERO)
            .or(holding.mark)
        else {
            return;
        };
        let limit = aggressive_limit(self.cfg.slippage_bps, Side::Sell, touch, holding.price_step);
        if limit <= Decimal::ZERO {
            eprintln!(
                "[EXEC] recovery: no executable price for {}; cannot sell it",
                holding.currency
            );
            return;
        }

        let snapshot = MarketSnapshot {
            symbol: holding.symbol.clone(),
            last_price: Some(mark),
            best_bid: holding.best_bid,
            best_ask: holding.best_ask,
            ..MarketSnapshot::default()
        };
        let request = OrderRequest {
            symbol: holding.symbol.clone(),
            side: Side::Sell,
            amount,
            price: limit,
            kind: OrderKind::Market,
            client_order_id: self.next_client_id(),
        };
        println!(
            "[EXEC] recovery: liquidating {} qty={} at market (limit {})",
            holding.symbol.to_uppercase(),
            amount,
            limit
        );

        let mut report = match self.broker.place(&request, &snapshot).await {
            Ok(report) => report,
            Err(error) => {
                eprintln!(
                    "[EXEC] recovery: {} liquidation failed: {error}",
                    holding.symbol.to_uppercase()
                );
                return;
            }
        };
        println!(
            "[EXEC] recovery order {} accepted: {}",
            report.order_id,
            report.status.as_str()
        );

        let deadline = Instant::now() + RECOVERY_FILL_TIMEOUT;
        while !report.status.is_terminal() && Instant::now() < deadline {
            tokio::time::sleep(RECOVERY_FILL_POLL).await;
            match self.broker.status(&report).await {
                Ok(updated) => report = updated,
                Err(error) => {
                    eprintln!(
                        "[EXEC] recovery: status for {} failed: {error}",
                        holding.symbol.to_uppercase()
                    );
                    break;
                }
            }
        }

        if !report.status.is_terminal() {
            match self.broker.cancel(&report).await {
                Ok(()) => {}
                Err(error) => eprintln!(
                    "[EXEC] recovery: could not cancel {}: {error}",
                    holding.symbol.to_uppercase()
                ),
            }
            tokio::time::sleep(RECOVERY_FILL_POLL).await;
            if let Ok(updated) = self.broker.status(&report).await {
                report = updated;
            }
        }

        if report.filled_amount > Decimal::ZERO {
            println!(
                "[EXEC] recovery: sold {} {} for {} USDT ({})",
                report.filled_amount,
                holding.symbol.to_uppercase(),
                report.filled_cash.round_dp(4),
                report.status.as_str()
            );
        } else {
            eprintln!(
                "[EXEC] recovery: {} liquidation ended {} with no fill; the balance remains",
                holding.symbol.to_uppercase(),
                report.status.as_str()
            );
        }
    }

    /// Fetches the live USDT balance and caches it for the AI context.
    async fn sync_live_balance(&mut self) -> Result<LiveBalance, ExecutionError> {
        self.last_balance_poll = Instant::now();
        let balances = self.broker.balances().await?;
        // A missing USDT row means the account holds no USDT, not a protocol
        // failure; the AI should still see `0`, never a dash.
        Ok(self.remember_live_balance(&balances))
    }

    /// Periodic refresh: keeps `equity`/`cash` in the AI context current.
    /// Skipped while a position or order is live so the REST call can never
    /// delay a take-profit/stop-loss decision; the fill path refreshes instead.
    async fn refresh_live_balance(&mut self) {
        if self.broker.is_paper()
            || self.working.is_some()
            || !self.portfolio.position.is_flat()
            || self.last_balance_poll.elapsed() < LIVE_BALANCE_POLL
        {
            return;
        }
        let previous = self.live_balance;
        match self.sync_live_balance().await {
            Ok(balance) => log_live_balance(previous, balance),
            Err(error) => eprintln!("[EXEC] balance refresh failed: {error}"),
        }
    }

    fn next_client_id(&self) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        format!("enrg-{nanos:x}")
    }

    /// Free quote currency: tracked locally in paper, fetched from HTX live.
    fn available_cash(&self) -> Option<Decimal> {
        if self.broker.is_paper() {
            Some(self.portfolio.cash)
        } else {
            self.live_balance.map(|balance| balance.available)
        }
    }

    /// Notional for the next entry. Live orders are additionally capped by the
    /// spendable USDT balance, so a small account is not rejected by the venue
    /// (or by the risk guard) for an order it cannot fund.
    fn entry_notional(&self, symbol: &str) -> Decimal {
        let configured = self.cfg.order_notional;
        if self.broker.is_paper() {
            return configured;
        }
        let Some(balance) = self.live_balance else {
            return configured;
        };
        let target = live_entry_notional(configured, balance.available, self.cfg.fee_rate());
        if target < configured {
            println!(
                "[EXEC] sizing {}: live balance {} USDT caps the order at {} USDT (configured {})",
                symbol.to_uppercase(),
                balance.available,
                target,
                configured
            );
        }
        target
    }

    /// Cash reported to the AI: frozen funds are still ours, so include them.
    fn account_cash(&self) -> Option<Decimal> {
        if self.broker.is_paper() {
            Some(self.portfolio.cash)
        } else {
            self.live_balance.map(LiveBalance::total)
        }
    }

    fn publish_account(&self) {
        let mark = self.last_mark;
        let qty = self.portfolio.position.qty();
        let view = AccountView {
            equity: if self.broker.is_paper() {
                Some(self.portfolio.equity(mark.unwrap_or_default()))
            } else {
                self.live_balance.map(|balance| {
                    live_equity(balance, qty, mark, self.portfolio.pending_cash_reserved())
                })
            },
            cash: self.account_cash(),
            position_symbol: self.open_symbol.clone(),
            position_qty: qty,
            avg_entry: self.portfolio.position.avg_entry(),
            unrealized: mark.map(|mark| self.portfolio.position.unrealized(mark)),
            realized_today: self.risk.realized_today(),
            trades: self.trades,
            halted: self.risk.is_halted(),
        };
        let _ = self.account_tx.send_replace(view);
    }

    fn report(&self, label: &str) {
        let equity = if self.broker.is_paper() {
            self.last_mark
                .map(|mark| self.portfolio.equity(mark))
                .unwrap_or(self.portfolio.cash)
                .round_dp(4)
                .to_string()
        } else {
            self.live_balance
                .map(|balance| {
                    live_equity(
                        balance,
                        self.portfolio.position.qty(),
                        self.last_mark,
                        self.portfolio.pending_cash_reserved(),
                    )
                    .round_dp(4)
                    .to_string()
                })
                .unwrap_or_else(|| "-".to_string())
        };
        println!(
            "[EXEC] {label}: mode={} equity={} cash={} position={} symbol={} avg_entry={} realized_today={} realized_total={} trades={} wins={} halted={}",
            self.broker.name(),
            equity,
            option_text(self.account_cash()),
            self.portfolio.position.qty(),
            self.open_symbol
                .as_deref()
                .map(|symbol| symbol.to_uppercase())
                .unwrap_or_else(|| "-".to_string()),
            self.portfolio.position.avg_entry(),
            self.risk.realized_today().round_dp(4),
            self.portfolio.position.realized_pnl().round_dp(4),
            self.trades,
            self.wins,
            if self.risk.is_halted() { "yes" } else { "no" },
        );
    }
}

/// Logs every non-zero venue balance so a restart shows the full account, not
/// only the USDT line.
fn log_balance_audit(balances: &[Balance]) {
    let mut any = false;
    for balance in balances {
        let total = balance.available + balance.frozen;
        if total <= Decimal::ZERO {
            continue;
        }
        any = true;
        println!(
            "[EXEC] balance audit: {} available={} frozen={} total={}",
            balance.currency, balance.available, balance.frozen, total
        );
    }
    if !any {
        println!("[EXEC] balance audit: the venue account is empty");
    }
}

/// Logs a live balance only when it actually moved.
fn log_live_balance(previous: Option<LiveBalance>, balance: LiveBalance) {
    if previous == Some(balance) {
        return;
    }
    println!(
        "[EXEC] live balance: USDT available={} frozen={} total={}",
        balance.available,
        balance.frozen,
        balance.total()
    );
}

/// The price an entry would touch: buys lift the ask, sells hit the bid.
pub(crate) fn entry_price(side: Side, snapshot: &MarketSnapshot) -> Option<Decimal> {
    match side {
        Side::Buy => snapshot.best_ask.or(snapshot.last_price),
        Side::Sell => snapshot.best_bid.or(snapshot.last_price),
    }
}

/// A post-only limit price that joins the book instead of crossing it.
///
/// Buys rest on the bid, sells on the ask, moved `offset_ticks` venue steps
/// towards the spread so the order sits in front of the queue instead of at its
/// back. The offset is only applied while it keeps the order passive: on a
/// one-tick spread it would cross, so the touch itself is used. Rounding is
/// always towards the passive side, so it can never turn a maker into a taker.
pub(crate) fn maker_price(
    side: Side,
    snapshot: &MarketSnapshot,
    price_step: Decimal,
    offset_ticks: u32,
) -> Option<Decimal> {
    let (touch, round_up) = match side {
        Side::Buy => (snapshot.best_bid?, false),
        Side::Sell => (snapshot.best_ask?, true),
    };
    if touch <= Decimal::ZERO || price_step <= Decimal::ZERO {
        return None;
    }
    let base = round_to_step(touch, price_step, round_up);
    let offset = price_step * Decimal::from(offset_ticks);
    let target = match side {
        Side::Buy => base + offset,
        Side::Sell => base - offset,
    };
    // Belt and braces: never hand a crossing price to a post-only venue.
    let price = if joins_the_book(side, target, snapshot) {
        target
    } else {
        base
    };
    (price > Decimal::ZERO && joins_the_book(side, price, snapshot)).then_some(price)
}

/// `true` when `price` stays on the passive side of the touch.
fn joins_the_book(side: Side, price: Decimal, snapshot: &MarketSnapshot) -> bool {
    match side {
        Side::Buy => snapshot.best_ask.is_none_or(|ask| price < ask),
        Side::Sell => snapshot.best_bid.is_none_or(|bid| price > bid),
    }
}

/// How far the mark has run above a resting entry's limit, in basis points.
fn entry_chase_bps(working: &TrackedOrder, snapshot: &MarketSnapshot) -> Option<Decimal> {
    let limit = working.request.price;
    if limit <= Decimal::ZERO {
        return None;
    }
    let mark = snapshot.mark_price()?;
    if mark <= limit {
        return Some(Decimal::ZERO);
    }
    Some((mark - limit) / limit * Decimal::from(10_000))
}

/// The price used to mark an exit for a position of `qty`.
pub(crate) fn exit_mark(qty: Decimal, snapshot: &MarketSnapshot) -> Option<Decimal> {
    let touch = if qty >= Decimal::ZERO {
        snapshot.best_bid
    } else {
        snapshot.best_ask
    };
    touch.or(snapshot.last_price)
}

/// Base-asset quantity for a target notional, rounded **down** to the venue
/// step so the order can never exceed the requested notional.
pub(crate) fn size_order(notional: Decimal, price: Decimal, step: Decimal) -> Decimal {
    if notional <= Decimal::ZERO || price <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let steps = (notional / price / step).trunc();
    if steps <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (steps * step).round_dp_with_strategy(step.scale(), RoundingStrategy::ToZero)
}

/// Marketable limit price: `touch` plus slippage, rounded outward so the order
/// always crosses the touch.
pub(crate) fn aggressive_limit(
    slippage_bps: Decimal,
    side: Side,
    touch: Decimal,
    price_step: Decimal,
) -> Decimal {
    let slippage = slippage_bps / Decimal::from(10_000);
    let raw = match side {
        Side::Buy => touch * (Decimal::ONE + slippage),
        Side::Sell => touch * (Decimal::ONE - slippage),
    };
    round_to_step(raw, price_step, side == Side::Buy)
}

fn round_to_step(value: Decimal, step: Decimal, up: bool) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    let steps = value / step;
    let rounded = if up { steps.ceil() } else { steps.floor() };
    (rounded * step).round_dp_with_strategy(step.scale(), RoundingStrategy::ToZero)
}

/// Formats an optional reading for a log line.
fn opt(value: Option<Decimal>, dp: u32) -> String {
    value
        .map(|value| value.round_dp(dp).to_string())
        .unwrap_or_else(|| "-".to_string())
}

/// Why a position was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    /// The mark fell `trail_atr_multiple` ATRs below the peak since entry.
    TrailingStop,
    /// The emergency stop: the position lost `stop_loss_pct`.
    StopLoss,
    /// The trend filter flipped against the long.
    TrendFlip,
}

impl ExitReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrailingStop => "atr-trailing-stop",
            Self::StopLoss => "hard-stop-loss",
            Self::TrendFlip => "trend-flip",
        }
    }
}

/// The exit rule as data, so the decision can be tested without an executor.
#[derive(Debug, Clone, Copy)]
pub struct ExitPolicy {
    pub stop_loss_pct: Decimal,
    pub trail_atr_multiple: Decimal,
    pub trail_fallback_pct: Decimal,
    pub trend_exit: bool,
}

impl ExitPolicy {
    pub fn from_config(cfg: &ExecutorConfig) -> Self {
        Self {
            stop_loss_pct: cfg.stop_loss_pct,
            trail_atr_multiple: cfg.trail_atr_multiple,
            trail_fallback_pct: cfg.trail_fallback_pct,
            trend_exit: cfg.trend_exit,
        }
    }

    /// How far below the peak the stop sits, in price units.
    ///
    /// With a warm ATR the distance is `trail_atr_multiple * ATR`, i.e. measured
    /// in the same unit as the market's own noise. Without one (a freshly adopted
    /// position, or a symbol that has not printed enough bars yet) the stop falls
    /// back to a percentage that is deliberately *wider* than the hard stop, so
    /// the emergency brake stays the binding constraint.
    pub fn trail_distance(&self, peak: Decimal, atr: Option<Decimal>) -> Decimal {
        match atr.filter(|atr| *atr > Decimal::ZERO) {
            Some(atr) => atr * self.trail_atr_multiple,
            None => peak * self.trail_fallback_pct / Decimal::from(100),
        }
    }

    /// The price the mark must not cross for the position to stay open.
    pub fn trail_floor(&self, peak: Decimal, atr: Option<Decimal>) -> Decimal {
        (peak - self.trail_distance(peak, atr)).max(Decimal::ZERO)
    }
}

/// Pure exit policy for a long position.
///
/// The order is deliberate:
///
/// 1. the hard stop — the one exit that must fire no matter what;
/// 2. the ATR trailing stop — hands the exit over to the market's own volatility;
/// 3. the trend flip — the reason the entry existed is gone.
///
/// There is no time-based exit and no fixed take-profit: a trend follower lets the
/// trailing stop decide when the move is over.
pub(crate) fn exit_decision(
    mark: Decimal,
    peak: Decimal,
    avg_entry: Decimal,
    atr: Option<Decimal>,
    trend_up: Option<bool>,
    policy: &ExitPolicy,
) -> Option<ExitReason> {
    if mark <= Decimal::ZERO || peak <= Decimal::ZERO || avg_entry <= Decimal::ZERO {
        return None;
    }

    let pnl_pct = (mark - avg_entry) / avg_entry * Decimal::from(100);
    if policy.stop_loss_pct > Decimal::ZERO && pnl_pct <= -policy.stop_loss_pct {
        return Some(ExitReason::StopLoss);
    }

    if mark <= policy.trail_floor(peak, atr) {
        return Some(ExitReason::TrailingStop);
    }

    if policy.trend_exit && trend_up == Some(false) {
        return Some(ExitReason::TrendFlip);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use crate::exchange::htx::MarketEvent;
    use crate::scanner::PairInfo;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn config() -> ExecutorConfig {
        ExecutorConfig {
            order_notional: dec("100"),
            min_order_notional: dec("5"),
            max_order_notional: dec("200"),
            max_position_notional: dec("200"),
            max_daily_loss: dec("50"),
            trail_atr_multiple: dec("2"),
            trail_fallback_pct: dec("1.5"),
            stop_loss_pct: dec("0.7"),
            trend_exit: true,
            cooldown: Duration::ZERO,
            gate_ttl: Duration::from_secs(60),
            order_ttl: Duration::from_secs(5),
            breakout_taker: true,
            maker_offset_ticks: 1,
            entry_max_chase_bps: dec("20"),
            slippage_bps: Decimal::ZERO,
            fee_bps: Decimal::ZERO,
            base_step: dec("0.000001"),
            price_step: dec("0.01"),
            paper_start_cash: dec("10000"),
        }
    }

    fn pair(symbol: &str, base_step: &str, price_step: &str) -> PairInfo {
        PairInfo {
            symbol: symbol.into(),
            last: dec("1"),
            change_pct: dec("2"),
            amplitude_pct: dec("5"),
            quote_volume: dec("10000000"),
            spread_bps: None,
            base_step: dec(base_step),
            price_step: dec(price_step),
        }
    }

    fn board_with(symbols: &[(&str, &str, &str)]) -> Arc<RwLock<MarketBoard>> {
        let mut board = MarketBoard::with_config(crate::strategy::StrategyConfig::default());
        board.set_pairs(
            symbols
                .iter()
                .map(|(symbol, base_step, price_step)| pair(symbol, base_step, price_step))
                .collect(),
        );
        Arc::new(RwLock::new(board))
    }

    fn publish(board: &Arc<RwLock<MarketBoard>>, symbol: &str, bid: &str, ask: &str, last: &str) {
        let mut board = board
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        board.apply(&MarketEvent::Depth {
            symbol: symbol.into(),
            best_bid: Some(dec(bid)),
            best_ask: Some(dec(ask)),
            exchange_ts_ms: 0,
            received_ns: 1,
        });
        board.apply(&MarketEvent::Trade {
            symbol: symbol.into(),
            price: dec(last),
            amount: Decimal::ONE,
            exchange_ts_ms: 0,
            received_ns: 2,
        });
    }

    fn executor(cfg: ExecutorConfig, board: Arc<RwLock<MarketBoard>>) -> Executor {
        let (tx, _rx) = watch::channel(AccountView::default());
        Executor::new(cfg, ExecutionMode::Paper, board, tx).expect("paper executor must build")
    }

    fn verdict(symbols: &[&str]) -> ExecutionCommand {
        ExecutionCommand::Verdict {
            action: AiAction::Trade,
            symbols: symbols.iter().map(|symbol| (*symbol).to_string()).collect(),
            reason: "test".into(),
        }
    }

    fn buy_signal(symbol: &str, price: &str) -> Signal {
        // The default test signal is the maker path: most executor tests care
        // about a resting entry, its pending overlay and its fill.
        Signal {
            symbol: symbol.into(),
            side: Side::Buy,
            price: dec(price),
            adx: dec("28"),
            atr_pct: dec("0.4"),
            volume_spike: true,
            kind: EntryKind::Pullback,
            reason: "test pullback".into(),
        }
    }

    fn breakout_signal(symbol: &str, price: &str) -> Signal {
        Signal {
            kind: EntryKind::Breakout,
            reason: "test breakout".into(),
            ..buy_signal(symbol, price)
        }
    }

    fn tracked_entry() -> TrackedOrder {
        TrackedOrder {
            request: OrderRequest {
                symbol: "btcusdt".into(),
                side: Side::Buy,
                amount: dec("0.001"),
                price: dec("65000"),
                kind: OrderKind::MakerLimit,
                client_order_id: "test".into(),
            },
            report: OrderReport {
                order_id: "1".into(),
                client_order_id: "test".into(),
                status: OrderStatus::Submitted,
                filled_amount: Decimal::ZERO,
                filled_cash: Decimal::ZERO,
            },
            purpose: Purpose::Entry,
            created_at: Instant::now(),
            applied_filled: Decimal::ZERO,
            pending_entry: false,
            cancel_attempts: 0,
            next_cancel_at: None,
        }
    }

    #[test]
    fn sizing_rounds_down_to_the_venue_step() {
        // 100 USDT / 65001 = 0.00153844... -> 1538 whole 0.000001 steps.
        assert_eq!(
            size_order(dec("100"), dec("65001"), dec("0.000001")),
            dec("0.001538")
        );
        // Coarse steps cannot round up past the notional.
        assert_eq!(
            size_order(dec("100"), dec("65001"), dec("0.001")),
            dec("0.001")
        );
        assert_eq!(
            size_order(dec("1"), dec("65001"), dec("0.001")),
            Decimal::ZERO
        );
    }

    #[test]
    fn live_usdt_balance_is_extracted_and_priced_into_equity() {
        let balances = vec![
            Balance {
                currency: "BTC".into(),
                available: dec("1"),
                frozen: Decimal::ZERO,
            },
            Balance {
                currency: "usdt".into(),
                available: dec("10.002001"),
                frozen: dec("0.5"),
            },
        ];
        let balance = usdt_balance(&balances).expect("USDT must be found");
        assert_eq!(balance.available, dec("10.002001"));
        assert_eq!(balance.total(), dec("10.502001"));
        assert_eq!(
            live_equity(balance, Decimal::ZERO, None, Decimal::ZERO),
            dec("10.502001")
        );
        assert_eq!(
            live_equity(balance, dec("2"), Some(dec("3")), Decimal::ZERO),
            dec("16.502001"),
            "an open position must be marked into equity"
        );
        assert_eq!(
            live_equity(balance, dec("2"), Some(dec("3")), dec("5")),
            dec("11.502001"),
            "frozen USDT backing a pending maker must not be counted twice"
        );
    }

    #[test]
    fn live_entries_are_capped_by_the_balance_and_fee() {
        // 10 / 1.002 = 9.98003992..., so the order fits the 10 USDT balance.
        assert_eq!(
            live_entry_notional(dec("25"), dec("10"), dec("0.002")),
            dec("9.98003992")
        );
        // A funded account keeps the configured notional.
        assert_eq!(
            live_entry_notional(dec("5"), dec("100"), dec("0.002")),
            dec("5")
        );
    }

    #[test]
    fn recovery_ignores_balances_below_the_venue_minimum() {
        // The exact residual from the bug report: 0.0000976 DNA at 0.04.
        let dust = recovered_holding("DNA", "0.0000976", "0.0000976", "0.04", "1", "0.0001");
        assert_eq!(dust.notional(), dec("0.000003904"));
        assert!(
            is_dust_holding(&dust),
            "a balance worth less than 1 USDT can never be sold"
        );

        // 24 DNA at 0.04 = 0.96 USDT is still below the venue minimum.
        let just_under = recovered_holding("DNA", "24", "24", "0.04", "1", "0.0001");
        assert!(is_dust_holding(&just_under));

        // Exactly 1 USDT is the minimum accepted order, not dust.
        let minimum = recovered_holding("DNA", "25", "25", "0.04", "1", "0.0001");
        assert!(!is_dust_holding(&minimum));

        // An unquoted balance has no value; it is reported separately.
        let unquoted = RecoveredHolding {
            mark: None,
            ..minimum
        };
        assert!(!is_dust_holding(&unquoted));
    }

    #[test]
    fn order_value_min_error_is_recognized() {
        let api = |code: &str, message: &str| ExecutionError::Api {
            code: code.into(),
            message: message.into(),
        };

        assert!(is_order_value_min_error(&api(
            "order-value-min-error",
            "order value cannot be less than 1 USDT"
        )));
        assert!(is_order_value_min_error(&api(
            "invalid-argument",
            "ORDER-VALUE-MIN-ERROR"
        )));
        assert!(!is_order_value_min_error(&api(
            "invalid-argument",
            "invalid symbol"
        )));
        assert!(!is_order_value_min_error(&ExecutionError::Http(
            "timeout".into()
        )));
    }

    fn spot_quote(
        symbol: &str,
        last: &str,
        bid: &str,
        ask: &str,
        base_step: &str,
        price_step: &str,
    ) -> SpotQuote {
        SpotQuote {
            symbol: symbol.into(),
            last: dec(last),
            best_bid: Some(dec(bid)),
            best_ask: Some(dec(ask)),
            base_step: dec(base_step),
            price_step: dec(price_step),
        }
    }

    fn recovered_holding(
        currency: &str,
        qty: &str,
        available: &str,
        mark: &str,
        base_step: &str,
        price_step: &str,
    ) -> RecoveredHolding {
        RecoveredHolding {
            currency: currency.into(),
            symbol: format!("{}usdt", currency.to_ascii_lowercase()),
            qty: dec(qty),
            available: dec(available),
            mark: Some(dec(mark)),
            best_bid: Some(dec(mark)),
            best_ask: Some(dec(mark)),
            base_step: dec(base_step),
            price_step: dec(price_step),
        }
    }

    #[test]
    fn recovery_adopts_the_largest_plain_coin_and_skips_usdt() {
        let balances = vec![
            Balance {
                currency: "USDT".into(),
                available: dec("2"),
                frozen: Decimal::ZERO,
            },
            Balance {
                currency: "USDC".into(),
                available: dec("10"),
                frozen: Decimal::ZERO,
            },
            Balance {
                currency: "DNA".into(),
                available: dec("50"),
                frozen: Decimal::ZERO,
            },
        ];
        let quotes = HashMap::from([
            (
                "usdcusdt".to_string(),
                spot_quote("usdcusdt", "1", "0.9999", "1.0001", "0.01", "0.0001"),
            ),
            (
                "dnausdt".to_string(),
                spot_quote("dnausdt", "0.04", "0.0399", "0.0401", "1", "0.0001"),
            ),
        ]);

        let holdings = recovery_holdings(&balances, &quotes, dec("0.000001"), dec("0.01"));
        assert_eq!(holdings.len(), 2, "USDT itself is not a position");
        assert!(holdings.iter().all(|holding| holding.currency != "USDT"));

        let primary = pick_primary_holding(&holdings).expect("DNA is manageable");
        assert_eq!(
            holdings[primary].currency, "DNA",
            "a stablecoin must never be adopted as the trading position"
        );
    }

    #[test]
    fn recovery_reports_an_unquoted_balance_but_does_not_adopt_it() {
        let balances = vec![
            Balance {
                currency: "MYSTERY".into(),
                available: dec("100"),
                frozen: Decimal::ZERO,
            },
            Balance {
                currency: "DNA".into(),
                available: dec("50"),
                frozen: Decimal::ZERO,
            },
        ];
        let quotes = HashMap::from([(
            "dnausdt".to_string(),
            spot_quote("dnausdt", "0.04", "0.0399", "0.0401", "1", "0.0001"),
        )]);

        let holdings = recovery_holdings(&balances, &quotes, dec("0.000001"), dec("0.01"));
        let mystery = holdings
            .iter()
            .find(|holding| holding.currency == "MYSTERY")
            .expect("an unknown balance must still be reported");
        assert!(mystery.mark.is_none());
        assert_eq!(mystery.base_step, dec("0.000001"));

        let primary = pick_primary_holding(&holdings).expect("DNA is manageable");
        assert_eq!(holdings[primary].currency, "DNA");
    }

    #[tokio::test]
    async fn recovered_position_is_closed_by_its_stop_loss() {
        // The recovered pair is deliberately absent from the watchlist: the pin
        // must add it to the websocket subscriptions and its steps to the board.
        let board = board_with(&[]);
        let mut executor = executor(config(), board.clone());
        let (subscriptions_tx, subscriptions_rx) = watch::channel(Vec::new());
        executor.subscriptions_tx = Some(subscriptions_tx);
        let holding = recovered_holding("DNA", "50", "50", "0.04", "1", "0.0001");

        assert!(executor.adopt_recovered_position(&holding));
        assert_eq!(executor.portfolio.position.qty(), dec("50"));
        assert_eq!(executor.portfolio.position.avg_entry(), dec("0.04"));
        assert_eq!(executor.open_symbol.as_deref(), Some("dnausdt"));
        assert!(executor.opened_at.is_some(), "the hold clock must start");
        assert_eq!(
            subscriptions_rx.borrow().as_slice(),
            ["dnausdt".to_string()].as_slice(),
            "recovery must subscribe the websocket to the held pair"
        );
        assert_eq!(
            executor.steps_for("dnausdt"),
            (dec("1"), dec("0.0001")),
            "recovery must keep the venue steps for the pinned pair"
        );

        // 0.0397 is 0.75% below the recovery mark, past the 0.7% hard stop.
        publish(&board, "dnausdt", "0.0397", "0.0398", "0.0397");
        executor.maybe_exit().await;

        assert!(
            executor.portfolio.position.is_flat(),
            "the recovered position must be sellable through the normal exit path"
        );
        assert_eq!(executor.open_symbol, None);
    }

    #[test]
    fn dropping_a_residual_position_resets_the_executor() {
        let board = board_with(&[]);
        let mut executor = executor(config(), board);
        let (subscriptions_tx, subscriptions_rx) = watch::channel(Vec::new());
        executor.subscriptions_tx = Some(subscriptions_tx);
        let holding = recovered_holding("DNA", "0.0000976", "0.0000976", "0.04", "1", "0.0001");

        assert!(executor.adopt_recovered_position(&holding));
        executor.open_fees = dec("0.01");
        assert_eq!(executor.peak_mark, Some(dec("0.04")));
        let cash = executor.portfolio.cash;

        executor.drop_tracked_position();

        assert!(executor.portfolio.position.is_flat());
        assert_eq!(executor.portfolio.cash, cash, "no fill means no cash move");
        assert_eq!(executor.open_symbol, None);
        assert_eq!(executor.opened_at, None);
        assert_eq!(executor.peak_mark, None);
        assert_eq!(executor.open_fees, Decimal::ZERO);
        assert_eq!(executor.exit_retry_after, None);
        assert!(
            subscriptions_rx.borrow().is_empty(),
            "a dropped position must leave the websocket watchlist"
        );
    }

    #[test]
    fn account_view_describes_live_equity_and_cash() {
        let view = AccountView {
            equity: Some(dec("9.98")),
            cash: Some(dec("10.002")),
            ..AccountView::default()
        };
        let text = view.describe();
        assert!(
            text.contains("equity=9.98 cash=10.002"),
            "the AI context must show live numbers, got {text:?}"
        );
    }

    #[test]
    fn live_balance_populates_the_ai_account_view() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        let rest = HtxRest::new("https://api.htx.com", "test-key", "test-secret")
            .expect("test rest client must build");
        let (tx, rx) = watch::channel(AccountView::default());
        let mut executor = Executor::new(config(), ExecutionMode::Paper, board, tx)
            .expect("paper executor must build");
        // Swap the paper venue for a live one without touching the network.
        executor.broker = Broker::live(rest);

        executor.live_balance = Some(LiveBalance {
            available: dec("10.002001"),
            frozen: Decimal::ZERO,
        });
        executor.publish_account();

        let view = rx.borrow().clone();
        assert_eq!(view.cash, Some(dec("10.002001")));
        assert_eq!(view.equity, Some(dec("10.002001")));
        assert!(
            view.describe().contains("equity=10.0020 cash=10.0020"),
            "the AI must see the live balance, got {:?}",
            view.describe()
        );
    }

    #[test]
    fn aggressive_limit_rounds_outward() {
        // Buy: round the 5 bps cushion up to the next cent.
        assert_eq!(
            aggressive_limit(dec("5"), Side::Buy, dec("65001"), dec("0.01")),
            dec("65033.51")
        );
        // Sell: round down, still crossing the bid.
        assert_eq!(
            aggressive_limit(dec("5"), Side::Sell, dec("65000"), dec("0.01")),
            dec("64967.5")
        );
        // No slippage keeps the touch, rounded to the step.
        assert_eq!(
            aggressive_limit(Decimal::ZERO, Side::Buy, dec("65000.004"), dec("0.01")),
            dec("65000.01")
        );
    }

    fn policy() -> ExitPolicy {
        ExitPolicy {
            stop_loss_pct: dec("0.7"),
            trail_atr_multiple: dec("2"),
            trail_fallback_pct: dec("1.5"),
            trend_exit: true,
        }
    }

    #[test]
    fn exit_policy_trails_by_atr_and_keeps_the_hard_stop() {
        let policy = policy();
        let entry = dec("100");
        let atr = Some(dec("0.5"));

        // 2 ATR under a 103 peak is 102: a mark above it keeps the trade alive.
        assert_eq!(
            exit_decision(dec("102.2"), dec("103"), entry, atr, Some(true), &policy),
            None
        );
        // Crossing the ATR trail closes the trade well above the entry.
        assert_eq!(
            exit_decision(dec("102"), dec("103"), entry, atr, Some(true), &policy),
            Some(ExitReason::TrailingStop)
        );
        // The hard stop is evaluated first and always wins.
        assert_eq!(
            exit_decision(dec("99.3"), dec("105"), entry, atr, Some(true), &policy),
            Some(ExitReason::StopLoss)
        );
        // Until the ATR is warm the fallback distance is slightly wider than the
        // hard stop, so the emergency brake stays the binding constraint.
        assert_eq!(
            exit_decision(dec("100.6"), dec("102"), entry, None, Some(true), &policy),
            None
        );
        assert_eq!(
            exit_decision(dec("100.4"), dec("102"), entry, None, Some(true), &policy),
            Some(ExitReason::TrailingStop)
        );
        // A trend flip closes the trade even when no stop is near.
        assert_eq!(
            exit_decision(dec("100.1"), dec("100.1"), entry, atr, Some(false), &policy),
            Some(ExitReason::TrendFlip)
        );
        // A book that is still warming has no trend reading: hold.
        assert_eq!(
            exit_decision(dec("100.1"), dec("100.1"), entry, atr, None, &policy),
            None
        );
    }

    #[test]
    fn the_trend_exit_can_be_disabled() {
        let mut policy = policy();
        policy.trend_exit = false;
        assert_eq!(
            exit_decision(
                dec("100.1"),
                dec("100.1"),
                dec("100"),
                Some(dec("0.5")),
                Some(false),
                &policy
            ),
            None
        );
    }

    #[test]
    fn the_gate_needs_a_fresh_trade_verdict_covering_the_symbol() {
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        let symbol = "btcusdt";

        let mut gate = Gate::closed();
        assert!(!gate.permits(symbol, ttl, now));

        gate.action = AiAction::Trade;
        gate.symbols = vec![symbol.into()];
        assert!(
            !gate.permits(symbol, ttl, now),
            "a verdict without a timestamp is stale"
        );

        gate.at = Some(now - Duration::from_secs(61));
        assert!(!gate.permits(symbol, ttl, now));

        gate.at = Some(now);
        assert!(gate.permits(symbol, ttl, now));
        assert!(
            !gate.permits("ethusdt", ttl, now),
            "a pair outside the AI list stays closed"
        );

        gate.symbols.push("ethusdt".into());
        assert!(gate.permits("btcusdt", ttl, now));
        assert!(
            gate.permits("ethusdt", ttl, now),
            "every pair the AI cleared opens the gate"
        );
    }

    #[test]
    fn the_gate_log_lists_every_cleared_pair() {
        assert_eq!(
            format_gate_symbols(&["btcusdt".into(), "listausdt".into(), "trumpusdt".into()]),
            "3 pairs: BTCUSDT, LISTAUSDT, TRUMPUSDT"
        );
        assert_eq!(format_gate_symbols(&["btcusdt".into()]), "1 pair: BTCUSDT");
        assert_eq!(format_gate_symbols(&[]), "no watchlist pairs");
    }

    #[tokio::test]
    async fn paper_round_trip_rides_the_trend_and_sells_on_the_retrace() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        assert!(executor.handle_command(verdict(&["btcusdt"])).await);
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;

        // The post-only buy rests one tick above the bid, but the local
        // portfolio already knows about it: no flat window between acceptance
        // and fill.
        let working = executor.working.as_ref().expect("maker order must rest");
        assert_eq!(working.request.kind, OrderKind::MakerLimit);
        assert_eq!(working.request.price, dec("65000.01"));
        assert!(working.pending_entry);
        assert_eq!(executor.portfolio.position.qty(), dec("0.001538"));
        assert_eq!(executor.portfolio.position.avg_entry(), dec("65000.01"));
        assert_eq!(
            executor.portfolio.cash,
            dec("9900.02998462"),
            "paper cash is reserved for the resting entry"
        );

        // The ask trades down to the resting bid: the maker order fills.
        publish(&board, "btcusdt", "64999", "65000", "65000");
        executor.on_tick().await;
        assert!(executor.working.is_none(), "maker order must fill");
        assert_eq!(executor.portfolio.position.qty(), dec("0.001538"));
        assert_eq!(executor.portfolio.position.avg_entry(), dec("65000"));
        assert!(executor.portfolio.cash < dec("10000"));
        assert_eq!(executor.open_symbol.as_deref(), Some("btcusdt"));

        // +2% on the bid sets the peak. Nothing is sold yet.
        publish(&board, "btcusdt", "66300", "66301", "66300.5");
        executor.on_tick().await;
        assert_eq!(executor.peak_mark, Some(dec("66300")));
        assert!(executor.working.is_none(), "no fixed take-profit exists");
        assert_eq!(
            executor.portfolio.position.qty(),
            dec("0.001538"),
            "the trend must be ridden, not sold at +2%"
        );

        // +3% sets a new peak; the position keeps running.
        publish(&board, "btcusdt", "66950", "66951", "66950.5");
        executor.on_tick().await;
        assert_eq!(executor.peak_mark, Some(dec("66950")));
        assert!(executor.portfolio.position.qty() > Decimal::ZERO);

        // 1.57% off the peak: the fallback stop (1.5% under the peak, because no
        // ATR is warm in this synthetic book) is crossed, so the trailing stop
        // hits the market and fills.
        publish(&board, "btcusdt", "65900", "65901", "65900.5");
        executor.on_tick().await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
        assert!(executor.open_symbol.is_none());
        assert_eq!(executor.peak_mark, None, "a closed trade resets the peak");
        assert_eq!(executor.trades, 1);
        assert_eq!(executor.wins, 1);
        assert!(executor.portfolio.position.realized_pnl() > Decimal::ZERO);
        assert!(executor.portfolio.cash > dec("10000"));
    }

    #[tokio::test]
    async fn a_retrace_inside_the_trailing_stop_keeps_the_trade_open() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["btcusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;
        publish(&board, "btcusdt", "64999", "65000", "65000");
        executor.on_tick().await;
        assert_eq!(executor.portfolio.position.avg_entry(), dec("65000"));

        // +1.85% sets a new peak; the fallback stop sits 1.5% under it.
        publish(&board, "btcusdt", "66200", "66201", "66200");
        executor.on_tick().await;
        assert!(executor.working.is_none());
        assert_eq!(executor.portfolio.position.qty(), dec("0.001538"));

        // A 1.06% retrace from that peak would have triggered a fixed take-profit
        // at some other setting, but it is still above the trailing stop.
        publish(&board, "btcusdt", "65500", "65501", "65500");
        executor.on_tick().await;
        assert!(executor.portfolio.position.qty() > Decimal::ZERO);
        assert!(executor.working.is_none());
        assert_eq!(executor.trades, 0);
    }

    #[tokio::test]
    async fn the_hard_stop_wins_over_the_trailing_stop() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["btcusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;
        publish(&board, "btcusdt", "64999", "65000", "65000");
        executor.on_tick().await;

        // The peak reaches +3%: the trailing stop now sits below 66950.
        publish(&board, "btcusdt", "66950", "66951", "66950");
        executor.on_tick().await;
        assert_eq!(executor.peak_mark, Some(dec("66950")));

        // The book collapses straight through the entry: the hard stop wins,
        // even though the peak is far above the mark.
        publish(&board, "btcusdt", "64350", "64351", "64350");
        executor.on_tick().await;

        assert!(executor.working.is_none());
        assert!(executor.portfolio.position.is_flat());
        assert_eq!(executor.trades, 1);
        assert_eq!(executor.wins, 0);
        assert!(executor.portfolio.position.realized_pnl() < Decimal::ZERO);
    }

    fn set_book_atr(board: &Arc<RwLock<MarketBoard>>, symbol: &str, atr: &str) {
        board
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_book_atr(symbol, dec(atr));
    }

    fn set_book_trend(board: &Arc<RwLock<MarketBoard>>, symbol: &str, up: bool) {
        board
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_book_trend(symbol, up);
    }

    #[tokio::test]
    async fn the_atr_trail_closes_the_position_two_atrs_below_the_peak() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["btcusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;
        publish(&board, "btcusdt", "64999", "65000", "65000");
        executor.on_tick().await;

        // The peak reaches 66950 and the book reports a 300-point ATR, so the
        // chandelier stop sits at 66950 - 2 * 300 = 66350.
        publish(&board, "btcusdt", "66950", "66951", "66950");
        executor.on_tick().await;
        set_book_atr(&board, "btcusdt", "300");
        assert_eq!(executor.peak_mark, Some(dec("66950")));

        // 60 points above the stop: the trend is still alive.
        publish(&board, "btcusdt", "66410", "66411", "66410");
        executor.on_tick().await;
        assert!(
            executor.working.is_none(),
            "the ATR trail must not fire early"
        );
        assert!(executor.portfolio.position.qty() > Decimal::ZERO);

        // 50 points below it: the trailing stop takes the market exit.
        publish(&board, "btcusdt", "66300", "66301", "66300");
        executor.on_tick().await;
        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
        assert_eq!(executor.trades, 1);
        assert_eq!(executor.wins, 1, "the ATR trail banks a +2% trend");
    }

    #[tokio::test]
    async fn a_trend_flip_closes_the_position() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["btcusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;
        publish(&board, "btcusdt", "64999", "65000", "65000");
        executor.on_tick().await;
        assert_eq!(executor.portfolio.position.avg_entry(), dec("65000"));

        // No stop is near, but the ribbon and the ADX gate no longer agree. The
        // trend is forced after the print, because every print re-evaluates it.
        publish(&board, "btcusdt", "64990", "64991", "64990");
        set_book_trend(&board, "btcusdt", false);
        executor.on_tick().await;

        assert!(
            executor.portfolio.position.is_flat(),
            "a dead trend is a reason to leave"
        );
        assert_eq!(executor.trades, 1);
    }

    #[tokio::test]
    async fn a_warming_book_never_exits_on_trend() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["btcusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;
        publish(&board, "btcusdt", "64999", "65000", "65000");
        executor.on_tick().await;

        // The synthetic book closes no bars, so trend_up is None: hold.
        assert!(executor.portfolio.position.qty() > Decimal::ZERO);
        assert_eq!(executor.trades, 0);
    }

    #[tokio::test]
    async fn cancelling_an_unfilled_maker_entry_rolls_the_portfolio_back() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["btcusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;

        assert_eq!(executor.portfolio.position.qty(), dec("0.001538"));
        assert_eq!(executor.portfolio.cash, dec("9900.02998462"));
        let working = executor.working.as_mut().expect("order must be resting");
        working.created_at = Instant::now() - config().order_ttl - Duration::from_secs(1);

        executor.on_tick().await;

        assert!(executor.working.is_none());
        assert!(executor.portfolio.position.is_flat());
        assert_eq!(executor.portfolio.cash, dec("10000"));
        assert_eq!(executor.open_symbol, None);
    }

    #[tokio::test]
    async fn closed_gate_blocks_entries() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board);

        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
        assert_eq!(executor.portfolio.cash, dec("10000"));
    }

    #[tokio::test]
    async fn a_pair_outside_the_ai_list_is_not_traded() {
        let board = board_with(&[
            ("btcusdt", "0.000001", "0.01"),
            ("listausdt", "1", "0.0001"),
        ]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        publish(&board, "listausdt", "0.13", "0.1301", "0.13");
        let mut executor = executor(config(), board.clone());

        // The AI cleared only LISTA; a BTC dislocation must not be traded.
        executor.handle_command(verdict(&["listausdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;
        assert!(executor.portfolio.position.is_flat());

        // The same AI verdict does allow the LISTA signal, on LISTA's steps.
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("listausdt", "0.1301")))
            .await;
        assert_eq!(executor.portfolio.position.qty(), dec("769"));
        assert_eq!(executor.open_symbol.as_deref(), Some("listausdt"));
        assert_eq!(
            executor
                .working
                .as_ref()
                .map(|working| working.request.kind),
            Some(OrderKind::MakerLimit)
        );
    }

    #[tokio::test]
    async fn a_multi_pair_verdict_trades_whichever_listed_pair_dislocates() {
        let board = board_with(&[
            ("btcusdt", "0.000001", "0.01"),
            ("listausdt", "1", "0.0001"),
            ("trumpusdt", "1", "0.0001"),
        ]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        publish(&board, "listausdt", "0.13", "0.1301", "0.13");
        publish(&board, "trumpusdt", "2", "2.001", "2");

        for (symbol, ask) in [("btcusdt", "65001"), ("listausdt", "0.1301")] {
            let mut executor = executor(config(), board.clone());
            executor
                .handle_command(verdict(&["btcusdt", "listausdt", "trumpusdt"]))
                .await;
            assert_eq!(executor.gate.symbols.len(), 3);

            executor
                .handle_command(ExecutionCommand::Signal(buy_signal(symbol, ask)))
                .await;

            let limit = executor
                .working
                .as_ref()
                .expect("the maker order must rest first")
                .request
                .price;
            assert_eq!(
                executor
                    .working
                    .as_ref()
                    .map(|working| working.request.kind),
                Some(OrderKind::MakerLimit),
                "every entry must be post-only"
            );
            assert!(
                executor.portfolio.position.qty() > Decimal::ZERO,
                "a signal on any listed pair must open a position immediately"
            );

            // The market trades through the resting bid and confirms the fill.
            let touch = limit.to_string();
            publish(&board, symbol, &touch, &touch, &touch);
            executor.on_tick().await;

            assert!(
                executor.working.is_none(),
                "the maker order on {symbol} must fill when the market crosses it"
            );
            assert!(
                executor.portfolio.position.qty() > Decimal::ZERO,
                "the position must survive the fill"
            );
            assert_eq!(executor.open_symbol.as_deref(), Some(symbol));
        }
    }

    #[tokio::test]
    async fn pairs_outside_the_watchlist_are_dropped_from_a_verdict() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board);

        executor
            .handle_command(verdict(&["btcusdt", "fakeusdt"]))
            .await;

        assert_eq!(executor.gate.symbols, vec!["btcusdt"]);
        assert!(executor.gate.at.is_some());
        assert!(
            !executor
                .gate
                .permits("fakeusdt", Duration::from_secs(60), Instant::now()),
            "a hallucinated pair must never open the gate"
        );

        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;
        assert_eq!(executor.open_symbol.as_deref(), Some("btcusdt"));
    }

    #[tokio::test]
    async fn bare_base_symbols_resolve_to_watchlist_pairs() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01"), ("xmrusdt", "0.01", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        publish(&board, "xmrusdt", "100", "100.01", "100");
        let mut executor = executor(config(), board);

        // DeepSeek sometimes returns `XMR` instead of `XMRUSDT`; the gate must
        // still match the streamed market, and a duplicate must not repeat.
        executor
            .handle_command(verdict(&["btcusdt", "xmr", "xmrusdt"]))
            .await;

        assert_eq!(executor.gate.symbols, vec!["btcusdt", "xmrusdt"]);

        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("xmrusdt", "100.01")))
            .await;
        assert_eq!(executor.open_symbol.as_deref(), Some("xmrusdt"));
    }

    #[tokio::test]
    async fn a_sell_signal_cannot_open_a_short_on_spot() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board);

        executor.handle_command(verdict(&["btcusdt"])).await;

        let signal = Signal {
            symbol: "btcusdt".into(),
            side: Side::Sell,
            price: dec("65000"),
            adx: dec("30"),
            atr_pct: dec("0.4"),
            volume_spike: true,
            kind: crate::strategy::EntryKind::Breakout,
            reason: "test breakout".into(),
        };
        executor
            .handle_command(ExecutionCommand::Signal(signal))
            .await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
    }

    #[tokio::test]
    async fn a_signal_for_an_unscanned_pair_is_ignored() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board);

        executor.handle_command(verdict(&["btcusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("ethusdt", "3000")))
            .await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
    }

    #[tokio::test]
    async fn halt_flattens_the_position_and_latches_the_guard() {
        let board = board_with(&[("listausdt", "1", "0.0001")]);
        publish(&board, "listausdt", "0.13", "0.1301", "0.13");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["listausdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("listausdt", "0.1301")))
            .await;
        assert!(executor.portfolio.position.qty() > Decimal::ZERO);

        let stopped = executor
            .handle_command(ExecutionCommand::Halt("test".into()))
            .await;

        assert!(!stopped, "halt must stop the executor loop");
        assert!(executor.portfolio.position.is_flat());
        assert!(executor.risk.is_halted());
        assert!(executor.open_symbol.is_none());
    }

    #[test]
    fn maker_prices_jump_the_queue_without_crossing() {
        let book = MarketSnapshot {
            symbol: "trumpusdt".into(),
            best_bid: Some(dec("0.9442")),
            best_ask: Some(dec("0.9457")),
            last_price: Some(dec("0.9442")),
            ..MarketSnapshot::default()
        };
        // One tick above the bid: in front of the queue, still passive.
        assert_eq!(
            maker_price(Side::Buy, &book, dec("0.0001"), 1),
            Some(dec("0.9443"))
        );
        // Zero offset keeps the old behaviour: the bid itself.
        assert_eq!(
            maker_price(Side::Buy, &book, dec("0.0001"), 0),
            Some(dec("0.9442"))
        );

        // A one-tick spread cannot be jumped without crossing, so the touch is
        // used instead of handing a taker price to a post-only venue.
        let tight = MarketSnapshot {
            symbol: "btcusdt".into(),
            best_bid: Some(dec("65000")),
            best_ask: Some(dec("65000.01")),
            last_price: Some(dec("65000.005")),
            ..MarketSnapshot::default()
        };
        assert_eq!(
            maker_price(Side::Buy, &tight, dec("0.01"), 2),
            Some(dec("65000"))
        );
    }

    #[test]
    fn cancel_backoff_grows_and_is_capped() {
        assert_eq!(cancel_backoff(1), Duration::from_secs(5));
        assert_eq!(cancel_backoff(3), Duration::from_secs(15));
        assert_eq!(cancel_backoff(12), CANCEL_BACKOFF_MAX);
        assert_eq!(cancel_backoff(u32::MAX), CANCEL_BACKOFF_MAX);
    }

    #[test]
    fn a_vanished_order_is_recognized() {
        assert!(is_order_not_found(&ExecutionError::Api {
            code: "order-order-not-exist".into(),
            message: "order not exist".into(),
        }));
        assert!(is_order_not_found(&ExecutionError::Api {
            code: "invalid-argument".into(),
            message: "order-not-exist".into(),
        }));
        assert!(!is_order_not_found(&ExecutionError::Api {
            code: "invalid-signature".into(),
            message: "bad signature".into(),
        }));
        assert!(!is_order_not_found(&ExecutionError::Http(
            "connection reset".into()
        )));
    }

    #[tokio::test]
    async fn repeated_cancel_failures_halt_entries() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board);
        let mut working = tracked_entry();

        for attempt in 1..CANCEL_ATTEMPT_LIMIT {
            executor.defer_cancel(&mut working, "test");
            assert_eq!(working.cancel_attempts, attempt);
            assert!(!executor.risk.is_halted(), "halt only at the limit");
            let wait = working
                .next_cancel_at
                .expect("a retry must be scheduled")
                .duration_since(Instant::now());
            assert!(wait <= CANCEL_BACKOFF_MAX);
        }

        // The last allowed attempt hands the order to an operator instead of
        // hammering the venue forever.
        executor.defer_cancel(&mut working, "test");
        assert_eq!(working.cancel_attempts, CANCEL_ATTEMPT_LIMIT);
        assert!(executor.risk.is_halted());
        assert!(executor
            .risk
            .halt_reason()
            .is_some_and(|reason| reason.contains("cannot be cancelled")));
    }

    #[tokio::test]
    async fn breakout_entries_take_the_ask_instead_of_resting() {
        let board = board_with(&[("trumpusdt", "1", "0.0001")]);
        publish(&board, "trumpusdt", "0.9442", "0.9457", "0.9442");
        let mut executor = executor(config(), board);

        executor.handle_command(verdict(&["trumpusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(breakout_signal(
                "trumpusdt",
                "0.9457",
            )))
            .await;

        // A breakout is momentum: the entry crosses the spread instead of
        // lingering under a move that may never come back.
        assert!(executor.working.is_none(), "market entry must fill at once");
        assert_eq!(executor.portfolio.position.qty(), dec("105"));
        assert_eq!(executor.portfolio.position.avg_entry(), dec("0.9457"));
        assert_eq!(executor.open_symbol.as_deref(), Some("trumpusdt"));
    }

    #[tokio::test]
    async fn pullback_entries_rest_one_tick_above_the_bid() {
        let board = board_with(&[("trumpusdt", "1", "0.0001")]);
        publish(&board, "trumpusdt", "0.9442", "0.9457", "0.9442");
        let mut executor = executor(config(), board);

        executor.handle_command(verdict(&["trumpusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("trumpusdt", "0.9442")))
            .await;

        let working = executor.working.as_ref().expect("maker order must rest");
        assert_eq!(working.request.kind, OrderKind::MakerLimit);
        assert_eq!(
            working.request.price,
            dec("0.9443"),
            "a pullback joins the front of the bid queue"
        );
        assert!(working.pending_entry);
    }

    #[tokio::test]
    async fn a_pending_cancel_backoff_suppresses_the_next_attempt() {
        let board = board_with(&[("trumpusdt", "1", "0.0001")]);
        publish(&board, "trumpusdt", "0.9442", "0.9457", "0.9442");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["trumpusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("trumpusdt", "0.9442")))
            .await;

        // A cancel already failed, so the next attempt is 5s away.
        let mut working = executor.working.take().expect("order must be resting");
        executor.defer_cancel(&mut working, "test");
        let attempts = working.cancel_attempts;
        executor.working = Some(working);

        // The market runs above the limit: the entry is stale on every tick, but
        // the pending backoff keeps the executor from hammering the venue.
        publish(&board, "trumpusdt", "0.9465", "0.9466", "0.9465");
        executor.on_tick().await;

        let working = executor
            .working
            .as_ref()
            .expect("the backoff must delay the next cancel");
        assert_eq!(working.cancel_attempts, attempts);
        assert!(working
            .next_cancel_at
            .is_some_and(|after| after > Instant::now()));
        assert!(!executor.portfolio.position.is_flat());
    }

    #[tokio::test]
    async fn a_resting_entry_is_cancelled_when_the_market_runs_away() {
        let board = board_with(&[("trumpusdt", "1", "0.0001")]);
        publish(&board, "trumpusdt", "0.9442", "0.9457", "0.9442");
        let mut executor = executor(config(), board.clone());

        executor.handle_command(verdict(&["trumpusdt"])).await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("trumpusdt", "0.9442")))
            .await;
        assert_eq!(executor.portfolio.position.qty(), dec("105"));

        // +23 bps above the resting limit: the signal no longer describes the
        // price, so the order is cancelled instead of waiting out its TTL.
        publish(&board, "trumpusdt", "0.9465", "0.9466", "0.9465");
        executor.on_tick().await;

        assert!(executor.working.is_none(), "stale entry must be cancelled");
        assert!(executor.portfolio.position.is_flat());
        assert_eq!(executor.portfolio.cash, dec("10000"));
        assert_eq!(executor.open_symbol, None);
    }
}
