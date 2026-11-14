//! Layer 4 — the executor.
//!
//! One task, one state machine:
//!
//! ```text
//! Signal (anomaly) ─┐
//!                   ├─> gate: fresh AI TRADE verdict?
//! AI verdict ───────┘        │
//!                            ├─> risk guard (size, exposure, cooldown, daily loss)
//!                            ├─> broker.place (paper or HTX)
//!                            └─> fills → portfolio/PnL → TP / SL / timeout exit
//! ```
//!
//! The executor never blocks the market-data path: it reads the freshest
//! [`MarketSnapshot`] for the symbol it is about to trade (or is holding) from
//! the shared [`MarketBoard`] and only speaks to the exchange from its own task.

use std::collections::HashSet;
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
use crate::execution::{ExecutionError, OrderReport, OrderRequest, OrderStatus, Side};
use crate::strategy::{MarketBoard, MarketSnapshot, Signal};

const TICK_INTERVAL: Duration = Duration::from_millis(250);
/// How often a live order is re-queried (HTX rate limits are per second).
const LIVE_STATUS_POLL: Duration = Duration::from_secs(1);
/// How often the live USDT balance is refreshed for the AI context.
const LIVE_BALANCE_POLL: Duration = Duration::from_secs(30);
/// Pause before re-attempting a failed or cancelled exit.
const EXIT_RETRY_DELAY: Duration = Duration::from_secs(2);

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
    pub take_profit_pct: Decimal,
    pub stop_loss_pct: Decimal,
    pub max_hold: Duration,
    pub cooldown: Duration,
    /// How long an AI verdict stays valid.
    pub gate_ttl: Duration,
    /// How long an unfilled entry order may rest before it is cancelled.
    pub order_ttl: Duration,
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
            take_profit_pct: env_decimal("EXEC_TAKE_PROFIT_PCT", "0.8")?,
            stop_loss_pct: env_decimal("EXEC_STOP_LOSS_PCT", "0.5")?,
            max_hold: env_duration("EXEC_MAX_HOLD_SECS", 120)?,
            cooldown: env_duration("EXEC_COOLDOWN_SECS", 10)?,
            gate_ttl: env_duration("EXEC_GATE_TTL_SECS", 120)?,
            order_ttl: env_duration("EXEC_ORDER_TTL_SECS", 5)?,
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
        if self.take_profit_pct <= Decimal::ZERO || self.stop_loss_pct <= Decimal::ZERO {
            return Err(ExecutionError::Config(
                "EXEC_TAKE_PROFIT_PCT and EXEC_STOP_LOSS_PCT must be positive".into(),
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
/// marked open position. Cash alone understates equity while an entry order is
/// resting, because the venue has moved part of the balance to `frozen`.
fn live_equity(balance: LiveBalance, qty: Decimal, mark: Option<Decimal>) -> Decimal {
    balance.total() + qty * mark.unwrap_or_default()
}

/// Caps the configured entry notional to what the live USDT balance can fund,
/// fee included. With `available=10` and a 20 bps fee the cap is 9.98003992.
fn live_entry_notional(configured: Decimal, available: Decimal, fee_rate: Decimal) -> Decimal {
    let affordable = (available / (Decimal::ONE + fee_rate)).round_dp(8);
    configured.min(affordable)
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
    exit_retry_after: Option<Instant>,
    last_mark: Option<Decimal>,
    last_status_poll: Instant,
    /// Latest USDT balance of the live venue, mirrored into the AI context.
    live_balance: Option<LiveBalance>,
    last_balance_poll: Instant,
    halt_logged: bool,
    account_tx: watch::Sender<AccountView>,
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
            exit_retry_after: None,
            last_mark: None,
            last_status_poll: Instant::now(),
            live_balance: None,
            last_balance_poll: Instant::now(),
            halt_logged: false,
            account_tx,
        })
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

    /// Keeps the open position's symbol streaming until it is flat.
    fn pin_board(&self, symbol: Option<String>) {
        self.board_mut().pin(symbol);
    }

    /// Runs until a [`ExecutionCommand::Halt`] arrives or the command channel
    /// closes.
    pub async fn run(mut self, mut commands: mpsc::Receiver<ExecutionCommand>) {
        println!(
            "[EXEC] mode={} pairs={} order_notional={}USDT tp={}% sl={}% max_hold={}s gate_ttl={}s",
            self.broker.name(),
            self.board().pairs().len(),
            self.cfg.order_notional,
            self.cfg.take_profit_pct,
            self.cfg.stop_loss_pct,
            self.cfg.max_hold.as_secs(),
            self.cfg.gate_ttl.as_secs(),
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

        let Some(touch) = entry_price(signal.side, &snapshot) else {
            println!(
                "[EXEC] skip {} {}: no top of book",
                symbol.to_uppercase(),
                signal.side.as_str()
            );
            return;
        };

        let (base_step, price_step) = self.steps_for(&symbol);
        let limit = aggressive_limit(self.cfg.slippage_bps, signal.side, touch, price_step);
        let target_notional = self.entry_notional(&symbol);
        // Size against the limit, not the touch: the fee and the slippage
        // cushion must fit the balance too.
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

        let request = OrderRequest {
            symbol: symbol.clone(),
            side: signal.side,
            amount,
            price: limit,
            client_order_id: self.next_client_id(),
        };

        let position_notional = self.portfolio.position.notional(touch);
        let cash = self.available_cash();
        if let Err(rejection) = self.risk.check(
            Intent::Entry,
            &request,
            touch,
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
            "[EXEC] placing {} {} {} @ {} (signal: {})",
            signal.side.as_str(),
            amount,
            symbol.to_uppercase(),
            limit,
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
        self.exit_retry_after = None;
        self.open_symbol = None;
        self.pin_board(None);
    }

    /// Periodic maintenance: fills, live status, order expiry, exits.
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

        // A working order that never filled must not camp on the book forever —
        // this covers both unfilled entries and stale exit limits.
        let expired = self.working.as_ref().is_some_and(|working| {
            !working.report.status.is_terminal()
                && working.created_at.elapsed() >= self.cfg.order_ttl
        });
        if expired {
            if let Some(working) = self.working.take() {
                println!(
                    "[EXEC] cancelling order {} after {}s without a fill",
                    working.report.order_id,
                    self.cfg.order_ttl.as_secs()
                );
                if let Err(error) = self.broker.cancel(&working.report).await {
                    eprintln!(
                        "[EXEC] cancel failed for {}: {error}",
                        working.report.order_id
                    );
                }
                let mut working = working;
                working.report.status = OrderStatus::Canceled;
                self.working = Some(working);
                self.absorb_working().await;
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

    /// Closes the position on take-profit, stop-loss or timeout, using the book
    /// of the symbol that is actually held.
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
        let pnl_pct = if qty > Decimal::ZERO {
            (mark - avg) / avg
        } else {
            (avg - mark) / avg
        } * Decimal::from(100);
        let held = self
            .opened_at
            .map(|at| now.duration_since(at))
            .unwrap_or_default();

        let Some(reason) = exit_decision(
            pnl_pct,
            held,
            self.cfg.take_profit_pct,
            self.cfg.stop_loss_pct,
            self.cfg.max_hold,
        ) else {
            return;
        };

        let side = if qty > Decimal::ZERO {
            Side::Sell
        } else {
            Side::Buy
        };
        let amount = qty.abs();
        let (_, price_step) = self.steps_for(&symbol);
        let limit = aggressive_limit(self.cfg.slippage_bps, side, mark, price_step);
        let request = OrderRequest {
            symbol: symbol.clone(),
            side,
            amount,
            price: limit,
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
            "[EXEC] closing position: {} {} {} @ {} — {} (pnl {}%)",
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
                });
                self.absorb_working().await;
            }
            Err(error) => {
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
            match self.broker.cancel(&working.report).await {
                Ok(()) => println!("[EXEC] cancelled working order {}", working.report.order_id),
                Err(error) => eprintln!(
                    "[EXEC] could not cancel {}: {error}",
                    working.report.order_id
                ),
            }
            // Book anything that filled before the cancel landed, so the
            // flatten order below covers the real position.
            let mut working = working;
            working.report.status = OrderStatus::Canceled;
            self.working = Some(working);
            self.absorb_working().await;
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

    /// Reads the live USDT balance at boot so a missing balance is visible
    /// before the first signal rather than after a rejected order.
    async fn live_preflight(&mut self) {
        match self.sync_live_balance().await {
            Ok(balance) => {
                println!("[EXEC] live balances: USDT={}", balance.available);
                if balance.available < self.cfg.order_notional {
                    eprintln!(
                        "[EXEC] WARNING: USDT balance {} is below the {} order notional; entries will be capped to the balance",
                        balance.available, self.cfg.order_notional
                    );
                }
            }
            Err(error) => eprintln!("[EXEC] could not load balances: {error}"),
        }
    }

    /// Fetches the live USDT balance and caches it for the AI context.
    async fn sync_live_balance(&mut self) -> Result<LiveBalance, ExecutionError> {
        self.last_balance_poll = Instant::now();
        let balances = self.broker.balances().await?;
        // A missing USDT row means the account holds no USDT, not a protocol
        // failure; the AI should still see `0`, never a dash.
        let balance = usdt_balance(&balances).unwrap_or_default();
        self.live_balance = Some(balance);
        Ok(balance)
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
                self.live_balance
                    .map(|balance| live_equity(balance, qty, mark))
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
                    live_equity(balance, self.portfolio.position.qty(), self.last_mark)
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

/// Why a position was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
    Timeout,
}

impl ExitReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TakeProfit => "take-profit",
            Self::StopLoss => "stop-loss",
            Self::Timeout => "max hold time",
        }
    }
}

/// Pure exit policy: take-profit wins over stop-loss, timeout last.
pub(crate) fn exit_decision(
    pnl_pct: Decimal,
    held: Duration,
    take_profit_pct: Decimal,
    stop_loss_pct: Decimal,
    max_hold: Duration,
) -> Option<ExitReason> {
    if take_profit_pct > Decimal::ZERO && pnl_pct >= take_profit_pct {
        Some(ExitReason::TakeProfit)
    } else if stop_loss_pct > Decimal::ZERO && pnl_pct <= -stop_loss_pct {
        Some(ExitReason::StopLoss)
    } else if !max_hold.is_zero() && held >= max_hold {
        Some(ExitReason::Timeout)
    } else {
        None
    }
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
            take_profit_pct: dec("0.5"),
            stop_loss_pct: dec("0.5"),
            max_hold: Duration::from_secs(60),
            cooldown: Duration::ZERO,
            gate_ttl: Duration::from_secs(60),
            order_ttl: Duration::from_secs(5),
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
        let mut board = MarketBoard::new();
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
        Signal {
            symbol: symbol.into(),
            side: Side::Buy,
            price: dec(price),
            move_pct: dec("1.2"),
            reason: "test dislocation".into(),
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
        assert_eq!(live_equity(balance, Decimal::ZERO, None), dec("10.502001"));
        assert_eq!(
            live_equity(balance, dec("2"), Some(dec("3"))),
            dec("16.502001"),
            "an open position must be marked into equity"
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

    #[test]
    fn exit_policy_picks_the_right_trigger() {
        let tp = dec("0.8");
        let sl = dec("0.5");
        let hold = Duration::from_secs(120);

        assert_eq!(
            exit_decision(dec("0.9"), Duration::ZERO, tp, sl, hold),
            Some(ExitReason::TakeProfit)
        );
        assert_eq!(
            exit_decision(dec("-0.6"), Duration::ZERO, tp, sl, hold),
            Some(ExitReason::StopLoss)
        );
        assert_eq!(
            exit_decision(dec("0.1"), Duration::from_secs(120), tp, sl, hold),
            Some(ExitReason::Timeout)
        );
        assert_eq!(
            exit_decision(dec("0.1"), Duration::from_secs(10), tp, sl, hold),
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
    async fn paper_round_trip_takes_profit() {
        let board = board_with(&[("btcusdt", "0.000001", "0.01")]);
        publish(&board, "btcusdt", "65000", "65001", "65000.5");
        let mut executor = executor(config(), board.clone());

        assert!(executor.handle_command(verdict(&["btcusdt"])).await);
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal("btcusdt", "65001")))
            .await;

        assert!(executor.working.is_none(), "marketable order must fill");
        assert_eq!(executor.portfolio.position.qty(), dec("0.001538"));
        assert_eq!(executor.portfolio.position.avg_entry(), dec("65001"));
        assert_eq!(executor.open_symbol.as_deref(), Some("btcusdt"));
        assert!(executor.portfolio.cash < dec("10000"));

        // +0.92% on the bid crosses the 0.5% take-profit.
        publish(&board, "btcusdt", "65600", "65601", "65600");
        executor.on_tick().await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
        assert!(executor.open_symbol.is_none());
        assert_eq!(executor.trades, 1);
        assert_eq!(executor.wins, 1);
        assert!(executor.portfolio.position.realized_pnl() > Decimal::ZERO);
        assert!(executor.portfolio.cash > dec("10000"));
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
        assert_eq!(executor.portfolio.position.qty(), dec("768"));
        assert_eq!(executor.open_symbol.as_deref(), Some("listausdt"));
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

            assert!(
                executor.working.is_none(),
                "the marketable order on {symbol} must fill"
            );
            assert!(
                executor.portfolio.position.qty() > Decimal::ZERO,
                "an anomaly on any listed pair must open a position"
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
            move_pct: dec("1.4"),
            reason: "test dislocation".into(),
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
}
