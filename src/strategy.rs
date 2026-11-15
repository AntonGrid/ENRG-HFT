//! Layer 2.5 — the shared market board.
//!
//! The board owns one rolling book per symbol. Market events arrive from a
//! multi-symbol WebSocket stream, are routed by their symbol and update that
//! symbol's bars, indicators and volume state.
//!
//! The strategy is a *trend follower*: it buys continuation, never a falling
//! knife. Bars of [`StrategyConfig::bar_secs`] seconds are aggregated from the
//! trade stream and feed four readings per symbol:
//!
//! * an EMA ribbon (fast/slow/trend) — the direction of the move;
//! * Wilder's ADX with +DI/-DI — whether there is a move at all;
//! * Wilder's ATR — how much noise the market carries, used by the exit;
//! * a rolling VWAP — the average price the flow has accepted.
//!
//! Two entries are possible, both long-only (spot never shorts):
//!
//! * [`EntryKind::Breakout`] — price crosses above the highest high of the last
//!   `breakout_bars` closed bars while the ribbon and the ADX gate agree;
//! * [`EntryKind::Pullback`] — inside an intact uptrend, a recent bar dipped to
//!   the fast EMA and price reclaims it.
//!
//! Volume never blocks an entry: a spike only marks the signal as higher
//! conviction for the AI layer and the logs.

use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use crate::exchange::htx::MarketEvent;
use crate::execution::Side;
use crate::quant_core::{Adx, Atr, Bar, Ema, Vwap};
use crate::scanner::PairInfo;

/// Hard cap on the bars a book keeps (24/7 market, so the ring is time-bounded
/// by `bar_secs * MAX_BARS`).
const MAX_BARS: usize = 240;

// --- volume window -------------------------------------------------------

const VOLUME_WINDOW_SECS: u64 = 120;
const VOLUME_WINDOW_NS: u128 = VOLUME_WINDOW_SECS as u128 * 1_000_000_000;
const VOLUME_RECENT_NS: u128 = 60 * 1_000_000_000;
const SECONDS_PER_DAY: u64 = 86_400;
const VOLUME_SPIKE_RATIO: u32 = 2;

/// How the strategy is tuned. Every field is fed from the environment so the bot
/// can be re-tuned without a rebuild (see `StrategyConfig::from_env`).
#[derive(Debug, Clone, Copy)]
pub struct StrategyConfig {
    /// Length of one aggregated bar, in seconds.
    pub bar_secs: i64,
    pub ema_fast: usize,
    pub ema_slow: usize,
    pub ema_trend: usize,
    pub adx_period: usize,
    /// Below this ADX the market is ranging and no entry is taken.
    pub adx_min: Decimal,
    pub atr_period: usize,
    pub breakout_bars: usize,
    pub vwap_bars: usize,
    /// How many recent bars count as a pullback into the fast EMA.
    pub pullback_bars: usize,
    pub cooldown: Duration,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            bar_secs: 60,
            ema_fast: 9,
            ema_slow: 21,
            ema_trend: 50,
            adx_period: 14,
            adx_min: Decimal::from(20),
            atr_period: 14,
            breakout_bars: 20,
            vwap_bars: 30,
            pullback_bars: 4,
            cooldown: Duration::from_secs(30),
        }
    }
}

impl StrategyConfig {
    /// Reads the `STRAT_*` variables, clamping every value into a sane range so a
    /// typo in `.env` can never make the bot trade noise.
    pub fn from_env() -> Self {
        let default = Self::default();
        let mut cfg = Self {
            bar_secs: env_usize("STRAT_BAR_SECS", default.bar_secs as usize).clamp(5, 3_600) as i64,
            ema_fast: env_usize("STRAT_EMA_FAST", default.ema_fast).clamp(2, 400),
            ema_slow: env_usize("STRAT_EMA_SLOW", default.ema_slow).clamp(3, 400),
            ema_trend: env_usize("STRAT_EMA_TREND", default.ema_trend).clamp(4, 800),
            adx_period: env_usize("STRAT_ADX_PERIOD", default.adx_period).clamp(2, 100),
            adx_min: env_decimal("STRAT_ADX_MIN", default.adx_min),
            atr_period: env_usize("STRAT_ATR_PERIOD", default.atr_period).clamp(1, 100),
            breakout_bars: env_usize("STRAT_BREAKOUT_BARS", default.breakout_bars).clamp(2, 200),
            vwap_bars: env_usize("STRAT_VWAP_BARS", default.vwap_bars).clamp(2, MAX_BARS),
            pullback_bars: env_usize("STRAT_PULLBACK_BARS", default.pullback_bars).clamp(1, 20),
            cooldown: Duration::from_secs(env_usize(
                "STRAT_COOLDOWN_SECS",
                default.cooldown.as_secs() as usize,
            ) as u64),
        };
        // The ribbon must stay ordered whatever was typed into the environment.
        if cfg.ema_fast >= cfg.ema_slow {
            cfg.ema_slow = cfg.ema_fast + 1;
        }
        if cfg.ema_slow >= cfg.ema_trend {
            cfg.ema_trend = cfg.ema_slow + 1;
        }
        cfg
    }

    /// Closed bars needed before the first reading is trustworthy: the widest
    /// warm-up of any indicator the entry rule uses.
    pub fn warmup_bars(&self) -> usize {
        self.ema_trend
            .max(self.adx_period * 2)
            .max(self.atr_period + 1)
            .max(self.breakout_bars + 1)
    }

    /// One-line summary used by the boot log.
    pub fn describe(&self) -> String {
        format!(
            "{}s bars, EMA {}/{}/{}, ADX({}) >= {}, ATR({}), Donchian({}), VWAP({}), warmup {} bars, cooldown {}s",
            self.bar_secs,
            self.ema_fast,
            self.ema_slow,
            self.ema_trend,
            self.adx_period,
            self.adx_min,
            self.atr_period,
            self.breakout_bars,
            self.vwap_bars,
            self.warmup_bars(),
            self.cooldown.as_secs(),
        )
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_decimal(name: &str, default: Decimal) -> Decimal {
    std::env::var(name)
        .ok()
        .and_then(|value| Decimal::from_str(value.trim()).ok())
        .unwrap_or(default)
}

/// Everything the exit logic and the AI layer need to know about one symbol.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MarketSnapshot {
    pub symbol: String,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub last_price: Option<Decimal>,
    pub ema_fast: Option<Decimal>,
    pub ema_slow: Option<Decimal>,
    pub ema_trend: Option<Decimal>,
    pub vwap: Option<Decimal>,
    pub atr: Option<Decimal>,
    /// ATR as a percentage of the last price — the unit the exits work in.
    pub atr_pct: Option<Decimal>,
    pub adx: Option<Decimal>,
    pub plus_di: Option<Decimal>,
    pub minus_di: Option<Decimal>,
    /// `true` when the ribbon and the ADX gate agree the market is trending up.
    pub trend_up: Option<bool>,
    /// Highest high of the last `breakout_bars` closed bars.
    pub breakout_level: Option<Decimal>,
    pub bars: usize,
    pub volume_1m: Decimal,
    pub volume_spike: bool,
    pub updated_ns: u128,
}

/// Which pattern produced a signal. Kept in the log so the post-mortem can tell
/// a breakout from a pullback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Breakout,
    Pullback,
}

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Breakout => "breakout",
            Self::Pullback => "pullback",
        }
    }
}

impl MarketSnapshot {
    /// Quoted spread in basis points, measured against the bid.
    pub fn spread_bps(&self) -> Option<Decimal> {
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO && ask >= bid => {
                Some((ask - bid) / bid * Decimal::from(10_000))
            }
            _ => None,
        }
    }

    /// The price used for marking positions: last trade, else the mid.
    pub fn mark_price(&self) -> Option<Decimal> {
        self.last_price
            .or_else(|| match (self.best_bid, self.best_ask) {
                (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
                (Some(bid), None) => Some(bid),
                (None, Some(ask)) => Some(ask),
                (None, None) => None,
            })
    }
}

/// One entry proposal. `adx` and `atr_pct` are the numbers that decided it, so
/// the log line is self-documenting.
#[derive(Debug, Clone, PartialEq)]
pub struct Signal {
    pub symbol: String,
    pub side: Side,
    pub kind: EntryKind,
    pub price: Decimal,
    pub adx: Decimal,
    pub atr_pct: Decimal,
    pub volume_spike: bool,
    pub reason: String,
}

struct Draft {
    side: Side,
    kind: EntryKind,
    price: Decimal,
    adx: Decimal,
    atr_pct: Decimal,
    reason: String,
}

#[derive(Default)]
pub struct MarketBoard {
    cfg: StrategyConfig,
    pairs: Vec<PairInfo>,
    active: HashSet<String>,
    books: HashMap<String, Book>,
    /// Venue steps per symbol, kept even for a pinned symbol the scanner has
    /// already rotated out.
    meta: HashMap<String, (Decimal, Decimal)>,
    /// Normal one-minute volume from the scanner's 24h figure.
    expected_minute_volume: HashMap<String, Decimal>,
    pinned: Option<String>,
}

impl MarketBoard {
    pub fn with_config(cfg: StrategyConfig) -> Self {
        Self {
            cfg,
            ..Self::default()
        }
    }

    /// Replaces the scanner watchlist and starts a book for every symbol.
    /// Symbols that left the list are pruned unless a position pins them.
    pub fn set_pairs(&mut self, pairs: Vec<PairInfo>) {
        let cfg = self.cfg;
        let mut active = HashSet::new();
        for pair in &pairs {
            active.insert(pair.symbol.clone());
            self.meta
                .insert(pair.symbol.clone(), (pair.base_step, pair.price_step));
            self.expected_minute_volume
                .insert(pair.symbol.clone(), expected_minute_volume(pair));
            self.books
                .entry(pair.symbol.clone())
                .or_insert_with(|| Book::new(cfg, &pair.symbol));
        }
        self.active = active;
        self.pairs = pairs;
        self.prune();
    }

    pub fn apply(&mut self, event: &MarketEvent) -> Option<Signal> {
        let symbol = event.symbol();
        let eligible = self.active.contains(symbol);
        if !eligible && self.pinned.as_deref() != Some(symbol) {
            return None;
        }

        let cfg = self.cfg;
        let expected = self
            .expected_minute_volume
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let book = self
            .books
            .entry(symbol.to_string())
            .or_insert_with(|| Book::new(cfg, symbol));
        let draft = book.on_event(event, expected)?;

        // A pinned-but-rotated-out position keeps streaming for its exit, but
        // it never opens a new trade.
        if !eligible {
            return None;
        }
        let volume_spike = book.snapshot.volume_spike;
        Some(Signal {
            symbol: symbol.to_string(),
            side: draft.side,
            kind: draft.kind,
            price: draft.price,
            adx: draft.adx,
            atr_pct: draft.atr_pct,
            volume_spike,
            reason: draft.reason,
        })
    }

    pub fn snapshot(&self, symbol: &str) -> Option<MarketSnapshot> {
        self.books.get(symbol).map(|book| book.snapshot.clone())
    }

    pub fn pairs(&self) -> &[PairInfo] {
        &self.pairs
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    pub fn is_active(&self, symbol: &str) -> bool {
        self.active.contains(symbol)
    }

    /// Symbols whose trades still change the books: the universe plus (via
    /// [`Self::pin`]) the symbols the bot currently holds.
    pub fn subscriptions(&self) -> Vec<String> {
        let mut symbols: Vec<String> = self.active.iter().cloned().collect();
        if let Some(symbol) = &self.pinned {
            if !self.active.contains(symbol) {
                symbols.push(symbol.clone());
            }
        }
        symbols.sort();
        symbols
    }

    /// Keeps a rotated-out symbol alive while an order of it is in flight. Its
    /// book keeps updating, but a pinned symbol never opens a new trade — that is
    /// the job of the exit logic that pinned it.
    pub fn pin(&mut self, symbol: Option<String>) {
        let cfg = self.cfg;
        self.pinned = symbol;
        if let Some(symbol) = &self.pinned {
            self.books
                .entry(symbol.clone())
                .or_insert_with(|| Book::new(cfg, symbol));
        }
        self.prune();
    }

    /// Venue steps for `symbol`, if the scanner or a recovered position knows
    /// them.
    pub fn steps(&self, symbol: &str) -> Option<(Decimal, Decimal)> {
        self.meta.get(symbol).copied()
    }

    /// Stores venue steps for a recovered position that is not on the scanner
    /// watchlist. Call after [`MarketBoard::pin`] so the later `prune` keeps it.
    pub fn set_steps(&mut self, symbol: &str, base_step: Decimal, price_step: Decimal) {
        self.meta
            .insert(symbol.to_string(), (base_step, price_step));
    }

    /// Test hook: forces the ATR reading of a book so the exit path can be
    /// exercised without feeding fifteen real bars.
    #[cfg(test)]
    pub fn set_book_atr(&mut self, symbol: &str, atr: Decimal) {
        if let Some(book) = self.books.get_mut(symbol) {
            book.snapshot.atr = Some(atr);
            book.snapshot.atr_pct = book
                .snapshot
                .last_price
                .filter(|price| *price > Decimal::ZERO)
                .map(|price| atr / price * Decimal::from(100));
        }
    }

    /// Test hook: forces the trend verdict of a book.
    #[cfg(test)]
    pub fn set_book_trend(&mut self, symbol: &str, trend_up: bool) {
        if let Some(book) = self.books.get_mut(symbol) {
            book.snapshot.trend_up = Some(trend_up);
        }
    }

    fn prune(&mut self) {
        let active = &self.active;
        let pinned = self.pinned.as_deref();
        let keep = |symbol: &String| active.contains(symbol) || pinned == Some(symbol.as_str());
        self.books.retain(|symbol, _| keep(symbol));
        self.meta.retain(|symbol, _| keep(symbol));
        self.expected_minute_volume.retain(|symbol, _| keep(symbol));
    }

    /// The multi-pair market context sent to DeepSeek: price, spread and 24h
    /// stats plus the trend reading that actually drives the entries.
    pub fn describe_pairs(&self) -> String {
        if self.pairs.is_empty() {
            return "watchlist: empty".to_string();
        }

        let cfg = self.cfg;
        let mut lines = Vec::with_capacity(self.pairs.len());
        for (index, pair) in self.pairs.iter().enumerate() {
            let snapshot = self.books.get(&pair.symbol).map(|book| &book.snapshot);
            let last = snapshot
                .and_then(|snapshot| snapshot.last_price)
                .unwrap_or(pair.last);
            let spread = snapshot
                .and_then(MarketSnapshot::spread_bps)
                .or(pair.spread_bps)
                .map(|value| format!("{}bps", value.round_dp(1).normalize()))
                .unwrap_or_else(|| "-".to_string());
            let trend = match snapshot.and_then(|snapshot| snapshot.trend_up) {
                Some(true) => "up",
                Some(false) => "down",
                None => "warming",
            };
            let value = |get: fn(&MarketSnapshot) -> Option<Decimal>| {
                snapshot
                    .and_then(get)
                    .map_or_else(|| "-".to_string(), |value| value.round_dp(6).to_string())
            };
            let atr_pct = snapshot
                .and_then(|snapshot| snapshot.atr_pct)
                .map(|value| format!("{}%", value.round_dp(3)))
                .unwrap_or_else(|| "-".to_string());
            let breakout = value(|snapshot| snapshot.breakout_level);

            lines.push(format!(
                "{}. {} last={} spread={} chg24={}% amp24={}% vol24={} vol1m={} spike={} trend={} ema{}={} ema{}={} ema{}={} vwap={} adx={} +di={} -di={} atr={} breakout={}",
                index + 1,
                pair.symbol.to_uppercase(),
                last,
                spread,
                pair.change_pct.round_dp(2),
                pair.amplitude_pct.round_dp(2),
                pair.quote_volume.round_dp(0),
                snapshot.map_or(Decimal::ZERO, |snapshot| snapshot.volume_1m.round_dp(0)),
                if snapshot.is_some_and(|snapshot| snapshot.volume_spike) {
                    "yes"
                } else {
                    "no"
                },
                trend,
                cfg.ema_fast,
                value(|snapshot| snapshot.ema_fast),
                cfg.ema_slow,
                value(|snapshot| snapshot.ema_slow),
                cfg.ema_trend,
                value(|snapshot| snapshot.ema_trend),
                value(|snapshot| snapshot.vwap),
                value(|snapshot| snapshot.adx),
                value(|snapshot| snapshot.plus_di),
                value(|snapshot| snapshot.minus_di),
                atr_pct,
                breakout,
            ));
        }
        lines.join("\n")
    }
}

/// One symbol's rolling state: closed bars, the forming bar and the indicators.
struct Book {
    cfg: StrategyConfig,
    bars: VecDeque<Bar>,
    current: Option<Bar>,
    current_bucket: i64,
    ema_fast: Ema,
    ema_slow: Ema,
    ema_trend: Ema,
    atr: Atr,
    adx: Adx,
    vwap: Vwap,
    volume_samples: VecDeque<(u128, Decimal)>,
    expected_minute_volume: Decimal,
    last_signal: Option<Instant>,
    snapshot: MarketSnapshot,
}

impl Book {
    fn new(cfg: StrategyConfig, symbol: &str) -> Self {
        Self {
            cfg,
            bars: VecDeque::with_capacity(MAX_BARS),
            current: None,
            current_bucket: 0,
            ema_fast: Ema::new(cfg.ema_fast),
            ema_slow: Ema::new(cfg.ema_slow),
            ema_trend: Ema::new(cfg.ema_trend),
            atr: Atr::new(cfg.atr_period),
            adx: Adx::new(cfg.adx_period),
            vwap: Vwap::new(cfg.vwap_bars),
            volume_samples: VecDeque::new(),
            expected_minute_volume: Decimal::ZERO,
            last_signal: None,
            snapshot: MarketSnapshot {
                symbol: symbol.to_string(),
                ..MarketSnapshot::default()
            },
        }
    }

    fn on_event(&mut self, event: &MarketEvent, expected_minute_volume: Decimal) -> Option<Draft> {
        match event {
            MarketEvent::Depth {
                best_bid,
                best_ask,
                received_ns,
                ..
            } => {
                self.snapshot.best_bid = *best_bid;
                self.snapshot.best_ask = *best_ask;
                self.snapshot.updated_ns = *received_ns;
                None
            }
            MarketEvent::Trade {
                price,
                amount,
                exchange_ts_ms,
                received_ns,
                ..
            } => self.on_trade(
                *price,
                *amount,
                *received_ns,
                *exchange_ts_ms,
                expected_minute_volume,
            ),
        }
    }

    fn on_trade(
        &mut self,
        price: Decimal,
        amount: Decimal,
        received_ns: u128,
        exchange_ts_ms: i64,
        expected_minute_volume: Decimal,
    ) -> Option<Draft> {
        if price <= Decimal::ZERO {
            return None;
        }
        let previous = self.snapshot.last_price;
        self.snapshot.last_price = Some(price);
        self.snapshot.updated_ns = received_ns;

        let quote = self.record_volume(price, amount, received_ns, expected_minute_volume);
        self.push_price(price, quote, event_ms(exchange_ts_ms, received_ns));
        self.evaluate(price, previous, Instant::now())
    }

    /// Tracks the last minute of quote volume against a two-minute baseline and
    /// flags a spike. The flag is *informational*: it never blocks a signal.
    fn record_volume(
        &mut self,
        price: Decimal,
        amount: Decimal,
        received_ns: u128,
        expected_minute_volume: Decimal,
    ) -> Decimal {
        let quote = price * amount;
        if quote <= Decimal::ZERO {
            self.snapshot.volume_spike = false;
            return Decimal::ZERO;
        }
        self.expected_minute_volume = expected_minute_volume;
        self.volume_samples.push_back((received_ns, quote));
        let cutoff = received_ns.saturating_sub(VOLUME_WINDOW_NS);
        while self
            .volume_samples
            .front()
            .is_some_and(|(at, _)| *at < cutoff)
        {
            self.volume_samples.pop_front();
        }

        let recent_cutoff = received_ns.saturating_sub(VOLUME_RECENT_NS);
        let mut recent = Decimal::ZERO;
        let mut previous = Decimal::ZERO;
        for (at, quote) in &self.volume_samples {
            if *at >= recent_cutoff {
                recent += *quote;
            } else {
                previous += *quote;
            }
        }
        self.snapshot.volume_1m = recent;

        let baseline = previous.max(expected_minute_volume);
        self.snapshot.volume_spike = if baseline > Decimal::ZERO {
            recent >= baseline * Decimal::from(VOLUME_SPIKE_RATIO)
        } else {
            false
        };
        quote
    }

    fn push_price(&mut self, price: Decimal, quote: Decimal, at_ms: i64) {
        let bar_ms = (self.cfg.bar_secs.max(1)) * 1_000;
        let bucket = if at_ms > 0 {
            at_ms / bar_ms
        } else {
            self.current_bucket
        };
        match self.current.take() {
            None => {
                self.current = Some(Bar::new(price, quote));
                if at_ms > 0 {
                    self.current_bucket = bucket;
                }
            }
            Some(mut bar) if at_ms <= 0 || bucket <= self.current_bucket => {
                bar.update(price, quote);
                self.current = Some(bar);
            }
            Some(bar) => {
                self.close_bar(bar);
                self.current = Some(Bar::new(price, quote));
                self.current_bucket = bucket;
            }
        }
    }

    fn close_bar(&mut self, bar: Bar) {
        self.bars.push_back(bar);
        while self.bars.len() > MAX_BARS {
            self.bars.pop_front();
        }

        self.ema_fast.update(bar.close);
        self.ema_slow.update(bar.close);
        self.ema_trend.update(bar.close);
        self.atr.update(&bar);
        self.adx.update(&bar);
        self.vwap.update(&bar);
        self.refresh();
    }

    fn refresh(&mut self) {
        self.snapshot.ema_fast = self.ema_fast.value();
        self.snapshot.ema_slow = self.ema_slow.value();
        self.snapshot.ema_trend = self.ema_trend.value();
        self.snapshot.vwap = self.vwap.value();
        self.snapshot.atr = self.atr.value();
        self.snapshot.atr_pct = self
            .snapshot
            .atr
            .zip(self.snapshot.last_price)
            .filter(|(_, price)| *price > Decimal::ZERO)
            .map(|(atr, price)| atr / price * Decimal::from(100));
        let reading = self.adx.reading();
        self.snapshot.adx = reading.map(|reading| reading.adx);
        self.snapshot.plus_di = reading.map(|reading| reading.plus_di);
        self.snapshot.minus_di = reading.map(|reading| reading.minus_di);
        self.snapshot.breakout_level = self.donchian_high();
        self.snapshot.bars = self.bars.len();
    }

    fn donchian_high(&self) -> Option<Decimal> {
        if self.bars.len() < self.cfg.breakout_bars {
            return None;
        }
        self.bars
            .iter()
            .rev()
            .take(self.cfg.breakout_bars)
            .map(|bar| bar.high)
            .max()
    }

    /// The entry decision. Runs on every print, so a level is never crossed
    /// without being seen; the indicators themselves only move on bar closes.
    fn evaluate(
        &mut self,
        price: Decimal,
        previous: Option<Decimal>,
        now: Instant,
    ) -> Option<Draft> {
        if self.bars.len() < self.cfg.warmup_bars() {
            self.snapshot.trend_up = None;
            return None;
        }
        let (Some(ema_fast), Some(ema_slow), Some(ema_trend)) = (
            self.ema_fast.value(),
            self.ema_slow.value(),
            self.ema_trend.value(),
        ) else {
            return None;
        };
        let reading = self.adx.reading()?;

        let ribbon_up = ema_fast > ema_slow && price > ema_trend;
        let adx_ok = reading.adx >= self.cfg.adx_min && reading.plus_di > reading.minus_di;
        let above_vwap = self.snapshot.vwap.is_none_or(|vwap| price > vwap);
        let trend_up = ribbon_up && adx_ok;
        self.snapshot.trend_up = Some(trend_up);

        if !trend_up || !above_vwap {
            return None;
        }
        if self
            .last_signal
            .is_some_and(|at| now.duration_since(at) < self.cfg.cooldown)
        {
            return None;
        }

        let previous = previous.unwrap_or(price);
        let atr_pct = self
            .snapshot
            .atr
            .filter(|atr| *atr > Decimal::ZERO)
            .map(|atr| atr / price * Decimal::from(100))
            .unwrap_or(Decimal::ZERO);
        let vwap_text = self
            .snapshot
            .vwap
            .map(|vwap| vwap.round_dp(8).to_string())
            .unwrap_or_else(|| "-".to_string());
        let volume_text = self.snapshot.volume_1m.round_dp(0);
        let spike_text = if self.snapshot.volume_spike {
            " (spike)"
        } else {
            ""
        };

        if let Some(level) = self.snapshot.breakout_level {
            if price > level && previous <= level {
                self.last_signal = Some(now);
                return Some(Draft {
                    side: Side::Buy,
                    kind: EntryKind::Breakout,
                    price,
                    adx: reading.adx,
                    atr_pct,
                    reason: format!(
                        "breakout above the {}-bar high {} with ema{} > ema{} and price above ema{}, adx {} (+DI {} > -DI {}), atr {}% of price, price above vwap {}, volume {} USDT/min{}",
                        self.cfg.breakout_bars,
                        level.round_dp(8),
                        self.cfg.ema_fast,
                        self.cfg.ema_slow,
                        self.cfg.ema_trend,
                        reading.adx.round_dp(1),
                        reading.plus_di.round_dp(1),
                        reading.minus_di.round_dp(1),
                        atr_pct.round_dp(3),
                        vwap_text,
                        volume_text,
                        spike_text,
                    ),
                });
            }
        }

        if self.pulled_back(ema_fast) && price > ema_fast && previous <= ema_fast {
            self.last_signal = Some(now);
            return Some(Draft {
                side: Side::Buy,
                kind: EntryKind::Pullback,
                price,
                adx: reading.adx,
                atr_pct,
                reason: format!(
                    "pullback reclaimed ema{} inside an intact uptrend (ema{} > ema{}, price above ema{}), adx {} (+DI {} > -DI {}), atr {}% of price, price above vwap {}, volume {} USDT/min{}",
                    self.cfg.ema_fast,
                    self.cfg.ema_fast,
                    self.cfg.ema_slow,
                    self.cfg.ema_trend,
                    reading.adx.round_dp(1),
                    reading.plus_di.round_dp(1),
                    reading.minus_di.round_dp(1),
                    atr_pct.round_dp(3),
                    vwap_text,
                    volume_text,
                    spike_text,
                ),
            });
        }

        None
    }

    /// `true` when one of the last `pullback_bars` closed bars traded down to the
    /// fast EMA — the dip the reclaim then has to recover from.
    fn pulled_back(&self, ema_fast: Decimal) -> bool {
        let depth = self.cfg.pullback_bars.max(1);
        self.bars
            .iter()
            .rev()
            .take(depth)
            .any(|bar| bar.low <= ema_fast || bar.close < ema_fast)
    }
}

/// The timestamp a print belongs to: the exchange one when the venue sends it,
/// otherwise the local receive time. `0` means "unknown", and then the print is
/// pinned to the bar that is already open.
fn event_ms(exchange_ts_ms: i64, received_ns: u128) -> i64 {
    if exchange_ts_ms > 0 {
        exchange_ts_ms
    } else if received_ns > 0 {
        (received_ns / 1_000_000) as i64
    } else {
        0
    }
}

/// Average quote volume per minute implied by the 24h turnover. HTX tickers
/// report rolling 24h volume, so this is a stable baseline even before the bot
/// has observed a full minute itself.
fn expected_minute_volume(pair: &PairInfo) -> Decimal {
    if pair.quote_volume <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    pair.quote_volume / Decimal::from(SECONDS_PER_DAY / 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    const START_MS: i64 = 1_700_000_000_000;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn pair(symbol: &str) -> PairInfo {
        PairInfo {
            symbol: symbol.to_string(),
            last: dec("100"),
            change_pct: dec("5"),
            amplitude_pct: dec("5"),
            quote_volume: dec("5000000"),
            spread_bps: None,
            base_step: dec("0.000001"),
            price_step: dec("0.01"),
        }
    }

    /// A board whose cooldown is disabled: the tests replay minutes of data in
    /// microseconds, so a wall-clock cooldown would swallow the later entries.
    fn board(symbol: &str) -> MarketBoard {
        board_with_cooldown(symbol, Duration::ZERO)
    }

    fn board_with_cooldown(symbol: &str, cooldown: Duration) -> MarketBoard {
        let mut board = MarketBoard::with_config(StrategyConfig {
            cooldown,
            ..StrategyConfig::default()
        });
        board.set_pairs(vec![pair(symbol)]);
        board
    }

    fn trade(symbol: &str, price: Decimal, amount: Decimal, at_ms: i64) -> MarketEvent {
        MarketEvent::Trade {
            symbol: symbol.to_string(),
            price,
            amount,
            exchange_ts_ms: at_ms,
            received_ns: at_ms as u128 * 1_000_000,
        }
    }

    /// Feeds one print per minute, exactly how a quiet 1m candle chart looks.
    fn feed(
        board: &mut MarketBoard,
        symbol: &str,
        start_ms: i64,
        prices: &[Decimal],
        amount: &str,
    ) -> Vec<Signal> {
        let amount = dec(amount);
        prices
            .iter()
            .enumerate()
            .filter_map(|(index, price)| {
                let at_ms = start_ms + index as i64 * 60_000;
                board.apply(&trade(symbol, *price, amount, at_ms))
            })
            .collect()
    }

    fn ramp(start: &str, step: &str, bars: usize) -> Vec<Decimal> {
        let start = dec(start);
        let step = dec(step);
        (0..bars)
            .map(|index| start + step * Decimal::from(index as u32))
            .collect()
    }

    #[test]
    fn nothing_is_traded_before_the_indicators_are_warm() {
        let mut board = board("btcusdt");
        let signals = feed(
            &mut board,
            "btcusdt",
            START_MS,
            &ramp("100", "0.3", 40),
            "10",
        );

        assert!(signals.is_empty(), "no entries before the warm-up is done");
        let snapshot = board.snapshot("btcusdt").expect("the book exists");
        assert_eq!(snapshot.trend_up, None);
        assert!(snapshot.bars < StrategyConfig::default().warmup_bars());
    }

    #[test]
    fn a_trend_breakout_produces_a_signal() {
        let mut board = board("btcusdt");
        let signals = feed(
            &mut board,
            "btcusdt",
            START_MS,
            &ramp("100", "0.3", 70),
            "10",
        );

        assert!(!signals.is_empty(), "a clean uptrend must produce entries");
        let signal = signals.last().expect("at least one signal");
        assert_eq!(signal.side, Side::Buy);
        assert_eq!(signal.kind, EntryKind::Breakout);
        assert!(signal.adx > dec("20"), "adx was {}", signal.adx);
        assert!(signal.atr_pct > Decimal::ZERO);
        assert!(signal.reason.contains("breakout above"));

        let snapshot = board.snapshot("btcusdt").expect("the book exists");
        assert_eq!(snapshot.trend_up, Some(true));
        assert!(snapshot.ema_fast > snapshot.ema_slow);
        assert!(snapshot.breakout_level.is_some());
    }

    #[test]
    fn a_pullback_inside_an_uptrend_is_bought_on_the_reclaim() {
        let mut board = board("btcusdt");
        let mut prices = ramp("100", "0.3", 60);
        // Dip under the fast EMA for two bars, then reclaim it.
        prices.extend([dec("116.6"), dec("115.8"), dec("116.2"), dec("117.2")]);
        let signals = feed(&mut board, "btcusdt", START_MS, &prices, "10");

        assert!(
            signals
                .iter()
                .any(|signal| signal.kind == EntryKind::Breakout),
            "the trend itself should have produced breakouts first"
        );
        let last = signals.last().expect("the reclaim must be an entry");
        assert_eq!(
            last.kind,
            EntryKind::Pullback,
            "kinds: {:?}",
            signals.iter().map(|signal| signal.kind).collect::<Vec<_>>()
        );
        assert!(last.reason.contains("pullback reclaimed"));
    }

    #[test]
    fn a_sideways_chop_produces_no_signals() {
        let mut board = board("btcusdt");
        let prices: Vec<Decimal> = (0..90)
            .map(|index| {
                if index % 2 == 0 {
                    dec("100")
                } else {
                    dec("100.3")
                }
            })
            .collect();
        let signals = feed(&mut board, "btcusdt", START_MS, &prices, "10");

        assert!(
            signals.is_empty(),
            "chop must not be traded, got {:?}",
            signals.iter().map(|signal| signal.kind).collect::<Vec<_>>()
        );
        let snapshot = board.snapshot("btcusdt").expect("the book exists");
        assert_eq!(snapshot.trend_up, Some(false));
    }

    #[test]
    fn a_downtrend_is_never_bought() {
        let mut board = board("btcusdt");
        let signals = feed(
            &mut board,
            "btcusdt",
            START_MS,
            &ramp("130", "-0.3", 80),
            "10",
        );

        assert!(
            signals.is_empty(),
            "spot is long-only, a downtrend is a no-trade"
        );
        let snapshot = board.snapshot("btcusdt").expect("the book exists");
        assert_eq!(snapshot.trend_up, Some(false));
    }

    #[test]
    fn volume_is_a_bonus_not_a_gate() {
        let prices = ramp("100", "0.3", 70);

        let mut quiet = board("btcusdt");
        let quiet_signals = feed(&mut quiet, "btcusdt", START_MS, &prices, "1");
        let quiet_last = quiet_signals
            .last()
            .expect("a trend without volume still trades");
        assert!(!quiet_last.volume_spike);

        let mut loud = board("btcusdt");
        let warm = feed(
            &mut loud,
            "btcusdt",
            START_MS,
            &prices[..prices.len() - 2],
            "1",
        );
        assert!(!warm.is_empty(), "the trend alone is enough to trade");
        let burst_start = START_MS + (prices.len() as i64 - 2) * 60_000;
        let burst = feed(
            &mut loud,
            "btcusdt",
            burst_start,
            &prices[prices.len() - 2..],
            "5000",
        );
        let burst_last = burst.last().expect("the burst keeps the trend tradable");
        assert!(
            burst_last.volume_spike,
            "a volume spike must be reported, volume_1m={}",
            loud.snapshot("btcusdt")
                .map(|s| s.volume_1m)
                .unwrap_or_default()
        );
    }

    #[test]
    fn the_ai_context_describes_the_trend() {
        let mut board = board("btcusdt");
        feed(
            &mut board,
            "btcusdt",
            START_MS,
            &ramp("100", "0.3", 70),
            "10",
        );

        let text = board.describe_pairs();
        assert!(text.contains("trend=up"), "{text}");
        assert!(text.contains("adx="), "{text}");
        assert!(text.contains("atr="), "{text}");
        assert!(text.contains("breakout="), "{text}");
    }

    #[test]
    fn a_pinned_position_keeps_its_book_without_entries() {
        let mut board = board("btcusdt");
        board.pin(Some("listausdt".to_string()));

        let prices = ramp("100", "0.3", 70);
        let mut signals = Vec::new();
        for (index, price) in prices.iter().enumerate() {
            let at_ms = START_MS + index as i64 * 60_000;
            signals.extend(board.apply(&trade("listausdt", *price, dec("10"), at_ms)));
        }

        assert!(
            signals.is_empty(),
            "a rotated-out position must never open a trade"
        );
        assert!(
            board
                .snapshot("listausdt")
                .is_some_and(|snapshot| snapshot.bars > 0),
            "but its book must stay live"
        );
        assert_eq!(
            board.subscriptions(),
            vec!["btcusdt".to_string(), "listausdt".to_string()]
        );
    }

    #[test]
    fn the_cooldown_stops_signal_spam_on_one_symbol() {
        let mut board = board_with_cooldown("btcusdt", Duration::from_secs(3_600));
        let signals = feed(
            &mut board,
            "btcusdt",
            START_MS,
            &ramp("100", "0.3", 70),
            "10",
        );

        assert_eq!(
            signals.len(),
            1,
            "one entry per cooldown window, got {}",
            signals.len()
        );
    }

    #[test]
    fn bars_are_aggregated_per_minute() {
        let mut board = board("btcusdt");
        // Three prints inside the same minute, then one in the next minute.
        for (price, at_ms) in [
            ("100", START_MS),
            ("101", START_MS + 20_000),
            ("99", START_MS + 40_000),
        ] {
            board.apply(&trade("btcusdt", dec(price), dec("1"), at_ms));
        }
        board.apply(&trade("btcusdt", dec("102"), dec("1"), START_MS + 60_000));

        let snapshot = board.snapshot("btcusdt").expect("the book exists");
        assert_eq!(snapshot.bars, 1, "only the finished minute is closed");
        assert_eq!(
            board.steps("btcusdt"),
            Some((dec("0.000001"), dec("0.01"))),
            "the venue steps must survive the rewrite"
        );
    }
}
