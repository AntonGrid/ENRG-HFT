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
//! [`MarketSnapshot`] from a `watch` channel and only speaks to the exchange
//! from its own task.

use std::str::FromStr;
use std::time::{Duration, Instant};

use chrono::Utc;
use rust_decimal::{Decimal, RoundingStrategy};
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, MissedTickBehavior};

use crate::ai_brain::AiAction;
use crate::exchange::htx;
use crate::execution::broker::Broker;
use crate::execution::htx_rest::HtxRest;
use crate::execution::portfolio::Portfolio;
use crate::execution::risk::{Intent, RiskGuard, RiskLimits};
use crate::execution::{ExecutionError, OrderReport, OrderRequest, OrderStatus, Side};
use crate::strategy::{MarketSnapshot, Signal};

const TICK_INTERVAL: Duration = Duration::from_millis(250);
/// How often a live order is re-queried (HTX rate limits are per second).
const LIVE_STATUS_POLL: Duration = Duration::from_secs(1);
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
    pub symbol: String,
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
    pub base_step: Decimal,
    pub price_step: Decimal,
    pub paper_start_cash: Decimal,
}

impl ExecutorConfig {
    pub fn from_env() -> Result<Self, ExecutionError> {
        let config = Self {
            symbol: htx::trading_symbol(),
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

/// A read-only view of the account, shared with the AI layer.
#[derive(Debug, Clone, Default)]
pub struct AccountView {
    pub equity: Option<Decimal>,
    pub cash: Option<Decimal>,
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
            "equity={} cash={} position_qty={} avg_entry={} unrealized={} realized_today={} trades={} halted={}",
            option_text(self.equity),
            option_text(self.cash),
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
    /// A market dislocation from the strategy.
    Signal(Signal),
    /// A fresh risk verdict from the AI layer.
    Verdict { action: AiAction, reason: String },
    /// Operator kill switch (Ctrl-C, fatal error elsewhere).
    Halt(String),
}

/// The AI gate: entries require a fresh `TRADE` verdict.
#[derive(Debug, Clone)]
struct Gate {
    action: AiAction,
    reason: String,
    at: Option<Instant>,
}

impl Gate {
    fn closed() -> Self {
        Self {
            action: AiAction::Pause,
            reason: "no AI verdict yet".into(),
            at: None,
        }
    }

    fn permits(&self, ttl: Duration, now: Instant) -> bool {
        self.action == AiAction::Trade && self.at.is_some_and(|at| now.duration_since(at) <= ttl)
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
    trades: u32,
    wins: u32,
    open_fees: Decimal,
    opened_at: Option<Instant>,
    exit_retry_after: Option<Instant>,
    last_mark: Option<Decimal>,
    last_status_poll: Instant,
    halt_logged: bool,
    account_tx: watch::Sender<AccountView>,
}

impl Executor {
    /// Builds an executor for the chosen mode. Live mode fails fast when the
    /// keys are absent instead of silently trading the paper book.
    pub fn new(
        cfg: ExecutorConfig,
        mode: ExecutionMode,
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
            trades: 0,
            wins: 0,
            open_fees: Decimal::ZERO,
            opened_at: None,
            exit_retry_after: None,
            last_mark: None,
            last_status_poll: Instant::now(),
            halt_logged: false,
            account_tx,
        })
    }

    /// Runs until a [`ExecutionCommand::Halt`] arrives or the command channel
    /// closes.
    pub async fn run(
        mut self,
        mut commands: mpsc::Receiver<ExecutionCommand>,
        market: watch::Receiver<MarketSnapshot>,
    ) {
        println!(
            "[EXEC] mode={} symbol={} order_notional={}USDT tp={}% sl={}% max_hold={}s gate_ttl={}s",
            self.broker.name(),
            self.cfg.symbol,
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
                    let snapshot = market.borrow().clone();
                    if !self.handle_command(command, &snapshot).await {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    let snapshot = market.borrow().clone();
                    self.on_tick(&snapshot).await;
                }
            }
        }

        self.report("final");
    }

    /// Handles one command. Returns `false` when the executor should stop.
    async fn handle_command(
        &mut self,
        command: ExecutionCommand,
        snapshot: &MarketSnapshot,
    ) -> bool {
        match command {
            ExecutionCommand::Signal(signal) => {
                self.try_enter(signal, snapshot).await;
                true
            }
            ExecutionCommand::Verdict { action, reason } => {
                println!("[EXEC] AI gate {} — {}", action.as_str(), reason);
                self.gate = Gate {
                    action,
                    reason,
                    at: Some(Instant::now()),
                };
                if action == AiAction::Pause && !self.portfolio.position.is_flat() {
                    println!(
                        "[EXEC] entries disabled by the AI; the open position keeps its TP/SL"
                    );
                }
                true
            }
            ExecutionCommand::Halt(reason) => {
                self.handle_halt(reason, snapshot).await;
                false
            }
        }
    }

    /// A dislocation arrived: gate, size, risk-check, place.
    async fn try_enter(&mut self, signal: Signal, snapshot: &MarketSnapshot) {
        if !self.portfolio.position.is_flat() {
            println!(
                "[EXEC] skip {}: position already open ({})",
                signal.side.as_str(),
                self.portfolio.position.qty()
            );
            return;
        }
        if self.working.is_some() {
            println!(
                "[EXEC] skip {}: an order is already working",
                signal.side.as_str()
            );
            return;
        }

        let now = Instant::now();
        if !self.gate.permits(self.cfg.gate_ttl, now) {
            println!(
                "[EXEC] skip {}: AI gate is {} ({})",
                signal.side.as_str(),
                self.gate.action.as_str(),
                self.gate.reason
            );
            return;
        }

        let Some(touch) = entry_price(signal.side, snapshot) else {
            println!("[EXEC] skip {}: no top of book", signal.side.as_str());
            return;
        };

        let amount = size_order(self.cfg.order_notional, touch, self.cfg.base_step);
        if amount <= Decimal::ZERO {
            println!(
                "[EXEC] skip: {} USDT buys less than one {}-step at {}",
                self.cfg.order_notional, self.cfg.base_step, touch
            );
            return;
        }

        let limit = aggressive_limit(
            self.cfg.slippage_bps,
            signal.side,
            touch,
            self.cfg.price_step,
        );
        let request = OrderRequest {
            symbol: self.cfg.symbol.clone(),
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
            if !self.portfolio.can_afford(notional, fee) {
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
            self.cfg.symbol,
            limit,
            signal.reason
        );

        match self.broker.place(&request, snapshot).await {
            Ok(report) => {
                println!(
                    "[EXEC] order {} accepted: {}",
                    report.order_id,
                    report.status.as_str()
                );
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
                    println!(
                        "[EXEC] entry fill {} {} @ {} (fee {})",
                        delta,
                        working.request.symbol,
                        price.round_dp(8),
                        fee.round_dp(8)
                    );
                }
                Purpose::Exit => {
                    println!(
                        "[EXEC] exit fill {} {} @ {} (fee {})",
                        delta,
                        working.request.symbol,
                        price.round_dp(8),
                        fee.round_dp(8)
                    );
                    if self.portfolio.position.is_flat() {
                        self.settle_trade(realized - fee);
                    }
                }
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
    }

    /// Periodic maintenance: fills, live status, order expiry, exits.
    async fn on_tick(&mut self, snapshot: &MarketSnapshot) {
        self.risk.roll_day(Utc::now().date_naive());
        if let Some(mark) = snapshot.mark_price() {
            self.last_mark = Some(mark);
        }

        // Paper fills happen on market moves; live fills arrive via polling.
        let updates = self.broker.on_market(snapshot);
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

        self.maybe_exit(snapshot).await;

        if self.risk.is_halted() && !self.halt_logged {
            println!(
                "[EXEC] entries halted: {}",
                self.risk.halt_reason().unwrap_or("unknown reason")
            );
            self.halt_logged = true;
        }

        self.publish_account();
    }

    /// Closes the position on take-profit, stop-loss or timeout.
    async fn maybe_exit(&mut self, snapshot: &MarketSnapshot) {
        if self.working.is_some() || self.portfolio.position.is_flat() {
            return;
        }

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

        let Some(mark) = exit_mark(qty, snapshot) else {
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
        let limit = aggressive_limit(self.cfg.slippage_bps, side, mark, self.cfg.price_step);
        let request = OrderRequest {
            symbol: self.cfg.symbol.clone(),
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
            "[EXEC] closing position: {} {} @ {} — {} (pnl {}%)",
            side.as_str(),
            amount,
            limit,
            reason.as_str(),
            pnl_pct.round_dp(3)
        );

        match self.broker.place(&request, snapshot).await {
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
    async fn handle_halt(&mut self, reason: String, snapshot: &MarketSnapshot) {
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
            let position = self.portfolio.position.clone();
            let qty = position.qty();
            let side = if qty > Decimal::ZERO {
                Side::Sell
            } else {
                Side::Buy
            };
            let touch = entry_price(side, snapshot).or(self.last_mark);

            if let Some(touch) = touch {
                let limit =
                    aggressive_limit(self.cfg.slippage_bps, side, touch, self.cfg.price_step);
                let request = OrderRequest {
                    symbol: self.cfg.symbol.clone(),
                    side,
                    amount: qty.abs(),
                    price: limit,
                    client_order_id: self.next_client_id(),
                };
                match self.broker.place(&request, snapshot).await {
                    Ok(report) => {
                        println!(
                            "[EXEC] kill switch: {} {} @ {} → order {} [{}]",
                            side.as_str(),
                            qty.abs(),
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

        self.publish_account();
    }

    /// Logs the live account balances once at boot so a missing balance is
    /// visible before the first signal rather than after a rejected order.
    async fn live_preflight(&self) {
        match self.broker.balances().await {
            Ok(balances) => {
                let quote = balances
                    .iter()
                    .find(|balance| balance.currency == "USDT")
                    .map(|balance| balance.available)
                    .unwrap_or(Decimal::ZERO);
                let base_currency = self
                    .cfg
                    .symbol
                    .strip_suffix("usdt")
                    .unwrap_or(&self.cfg.symbol)
                    .to_ascii_uppercase();
                let base = balances
                    .iter()
                    .find(|balance| balance.currency == base_currency)
                    .map(|balance| balance.available)
                    .unwrap_or(Decimal::ZERO);

                println!("[EXEC] live balances: USDT={quote} {base_currency}={base}");
                if quote < self.cfg.order_notional {
                    eprintln!(
                        "[EXEC] WARNING: USDT balance {quote} is below the {} order notional",
                        self.cfg.order_notional
                    );
                }
            }
            Err(error) => eprintln!("[EXEC] could not load balances: {error}"),
        }
    }

    fn next_client_id(&self) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        format!("enrg-{nanos:x}")
    }

    /// Cash is only known locally in paper mode.
    fn available_cash(&self) -> Option<Decimal> {
        self.broker.is_paper().then_some(self.portfolio.cash)
    }

    fn publish_account(&self) {
        let mark = self.last_mark;
        let view = AccountView {
            equity: if self.broker.is_paper() {
                mark.map(|mark| self.portfolio.equity(mark))
            } else {
                None
            },
            cash: self.available_cash(),
            position_qty: self.portfolio.position.qty(),
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
            "-".to_string()
        };
        println!(
            "[EXEC] {label}: mode={} equity={} cash={} position={} avg_entry={} realized_today={} realized_total={} trades={} wins={} halted={}",
            self.broker.name(),
            equity,
            option_text(self.available_cash()),
            self.portfolio.position.qty(),
            self.portfolio.position.avg_entry(),
            self.risk.realized_today().round_dp(4),
            self.portfolio.position.realized_pnl().round_dp(4),
            self.trades,
            self.wins,
            if self.risk.is_halted() { "yes" } else { "no" },
        );
    }
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

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn config() -> ExecutorConfig {
        ExecutorConfig {
            symbol: "btcusdt".into(),
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

    fn snapshot(bid: &str, ask: &str, last: &str) -> MarketSnapshot {
        MarketSnapshot {
            symbol: "btcusdt".into(),
            last_price: Some(dec(last)),
            best_bid: Some(dec(bid)),
            best_ask: Some(dec(ask)),
            ..MarketSnapshot::default()
        }
    }

    fn executor(cfg: ExecutorConfig) -> Executor {
        let (tx, _rx) = watch::channel(AccountView::default());
        Executor::new(cfg, ExecutionMode::Paper, tx).expect("paper executor must build")
    }

    fn buy_signal() -> Signal {
        Signal {
            side: Side::Buy,
            price: dec("65001"),
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
    fn the_gate_needs_a_fresh_trade_verdict() {
        let now = Instant::now();
        let ttl = Duration::from_secs(60);

        let mut gate = Gate::closed();
        assert!(!gate.permits(ttl, now));

        gate.action = AiAction::Trade;
        assert!(
            !gate.permits(ttl, now),
            "a verdict without a timestamp is stale"
        );

        gate.at = Some(now - Duration::from_secs(61));
        assert!(!gate.permits(ttl, now));

        gate.at = Some(now);
        assert!(gate.permits(ttl, now));
    }

    #[tokio::test]
    async fn paper_round_trip_takes_profit() {
        let mut executor = executor(config());
        let entry = snapshot("65000", "65001", "65000.5");

        assert!(
            executor
                .handle_command(
                    ExecutionCommand::Verdict {
                        action: AiAction::Trade,
                        reason: "test".into(),
                    },
                    &entry,
                )
                .await
        );
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal()), &entry)
            .await;

        assert!(executor.working.is_none(), "marketable order must fill");
        assert_eq!(executor.portfolio.position.qty(), dec("0.001538"));
        assert_eq!(executor.portfolio.position.avg_entry(), dec("65001"));
        assert!(executor.portfolio.cash < dec("10000"));

        // +0.92% on the bid crosses the 0.5% take-profit.
        let exit = snapshot("65600", "65601", "65600");
        executor.on_tick(&exit).await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
        assert_eq!(executor.trades, 1);
        assert_eq!(executor.wins, 1);
        assert!(executor.portfolio.position.realized_pnl() > Decimal::ZERO);
        assert!(executor.portfolio.cash > dec("10000"));
    }

    #[tokio::test]
    async fn closed_gate_blocks_entries() {
        let mut executor = executor(config());
        let entry = snapshot("65000", "65001", "65000.5");

        executor
            .handle_command(ExecutionCommand::Signal(buy_signal()), &entry)
            .await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
        assert_eq!(executor.portfolio.cash, dec("10000"));
    }

    #[tokio::test]
    async fn a_sell_signal_cannot_open_a_short_on_spot() {
        let mut executor = executor(config());
        let entry = snapshot("65000", "65001", "65000.5");
        executor
            .handle_command(
                ExecutionCommand::Verdict {
                    action: AiAction::Trade,
                    reason: "test".into(),
                },
                &entry,
            )
            .await;

        let signal = Signal {
            side: Side::Sell,
            price: dec("65000"),
            move_pct: dec("1.4"),
            reason: "test dislocation".into(),
        };
        executor
            .handle_command(ExecutionCommand::Signal(signal), &entry)
            .await;

        assert!(executor.portfolio.position.is_flat());
        assert!(executor.working.is_none());
    }

    #[tokio::test]
    async fn halt_flattens_the_position_and_latches_the_guard() {
        let mut executor = executor(config());
        let entry = snapshot("65000", "65001", "65000.5");
        executor
            .handle_command(
                ExecutionCommand::Verdict {
                    action: AiAction::Trade,
                    reason: "test".into(),
                },
                &entry,
            )
            .await;
        executor
            .handle_command(ExecutionCommand::Signal(buy_signal()), &entry)
            .await;
        assert!(executor.portfolio.position.qty() > Decimal::ZERO);

        let stopped = executor
            .handle_command(ExecutionCommand::Halt("test".into()), &entry)
            .await;

        assert!(!stopped, "halt must stop the executor loop");
        assert!(executor.portfolio.position.is_flat());
        assert!(executor.risk.is_halted());
    }
}
