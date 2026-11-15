//! Layer 2.5 — the shared market board.
//!
//! The board owns one rolling book per symbol. Market events arrive from a
//! multi-symbol WebSocket stream, are routed by their symbol and update that
//! symbol's window, top of book and anomaly state.
//!
//! Two consumers read the same board:
//!
//! * the strategy emits a [`Signal`] with the symbol that produced it. Spot is
//!   long-only: only a downside dislocation produces `BUY`, and `SELL` is left
//!   entirely to the executor's exit path for an already-open position;
//! * the executor reads the freshest [`MarketSnapshot`] for the symbol it is
//!   about to trade or is currently holding, and the AI layer reads
//!   [`MarketBoard::describe_pairs`] for its minute-by-minute verdict.
//!
//! The scanner's watchlist is mirrored into [`MarketBoard::pairs`]; only pairs in
//! `active` may produce entries, while a `pinned` symbol (the open position)
//! keeps streaming until it is flat even if the scanner has rotated it out.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use crate::exchange::htx::MarketEvent;
use crate::execution::Side;
use crate::quant_core;
use crate::scanner::PairInfo;

/// Minimum time between two anomaly signals on the same symbol.
pub const ANOMALY_COOLDOWN: Duration = Duration::from_secs(5);

/// How much trade history the volume filter keeps.
const VOLUME_WINDOW_NS: u128 = 120_000_000_000;
/// The "recent" arm of the volume comparison.
const VOLUME_RECENT_NS: u128 = 60_000_000_000;
/// A dip is only tradable when the last minute carries at least this multiple
/// of the normal one-minute flow.
const VOLUME_SPIKE_MULTIPLIER: Decimal = Decimal::TWO;
/// Upper bound on retained prints per symbol; deep enough for a very busy
/// minute, small enough that a forgotten book cannot grow without limit.
const VOLUME_SAMPLES_CAP: usize = 8192;
/// HTX tickers report rolling 24h volume; dividing by minutes gives a stable
/// fallback baseline even before the bot has observed a full minute itself.
const MINUTES_PER_DAY: u64 = 1_440;

/// Everything the rest of the bot needs to know about one symbol right now.
#[derive(Debug, Clone, Default)]
pub struct MarketSnapshot {
    pub symbol: String,
    pub last_price: Option<Decimal>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub window_low: Option<Decimal>,
    pub window_high: Option<Decimal>,
    /// Range of the anomaly window in percent (`1.25` means 1.25%).
    pub move_pct: Option<Decimal>,
    /// `true` when the latest tick set off the anomaly detector.
    pub anomaly: bool,
    /// Quote (USDT) volume printed over the last 60 seconds.
    pub volume_1m: Decimal,
    /// Normal one-minute volume used by the spike test.
    pub volume_baseline_1m: Decimal,
    /// `true` when `volume_1m` is at least twice the baseline.
    pub volume_spike: bool,
    pub updated_ns: u128,
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

/// A long-only entry opportunity. The executor decides whether to act.
#[derive(Debug, Clone)]
pub struct Signal {
    pub symbol: String,
    pub side: Side,
    pub price: Decimal,
    pub move_pct: Decimal,
    /// `true` when the dip arrived with a genuine volume spike.
    pub volume_spike: bool,
    pub reason: String,
}

/// One executed print, kept for the rolling volume calculation.
#[derive(Debug, Clone, Copy)]
struct VolumeSample {
    at_ns: u128,
    quote: Decimal,
}

/// Rolling state for one symbol.
#[derive(Debug)]
struct Book {
    window: VecDeque<Decimal>,
    volume: VecDeque<VolumeSample>,
    expected_minute_volume: Decimal,
    snapshot: MarketSnapshot,
    last_anomaly: Option<Instant>,
}

impl Book {
    fn new(symbol: &str, expected_minute_volume: Decimal) -> Self {
        Self {
            window: VecDeque::with_capacity(quant_core::ANOMALY_WINDOW_TICKS),
            volume: VecDeque::new(),
            expected_minute_volume,
            snapshot: MarketSnapshot {
                symbol: symbol.to_string(),
                ..MarketSnapshot::default()
            },
            last_anomaly: None,
        }
    }

    /// Returns `(side, price, move_pct, reason)` on a fresh dip. The side is
    /// always [`Side::Buy`]: spot never enters short.
    fn on_event(&mut self, event: &MarketEvent) -> Option<(Side, Decimal, Decimal, String)> {
        match event {
            MarketEvent::Trade {
                price,
                amount,
                received_ns,
                ..
            } => self.on_trade(*price, *amount, *received_ns),
            MarketEvent::Depth {
                best_bid,
                best_ask,
                received_ns,
                ..
            } => {
                if best_bid.is_some() {
                    self.snapshot.best_bid = *best_bid;
                }
                if best_ask.is_some() {
                    self.snapshot.best_ask = *best_ask;
                }
                self.snapshot.updated_ns = *received_ns;
                None
            }
        }
    }

    fn on_trade(
        &mut self,
        price: Decimal,
        amount: Decimal,
        received_ns: u128,
    ) -> Option<(Side, Decimal, Decimal, String)> {
        self.snapshot.last_price = Some(price);
        self.snapshot.updated_ns = received_ns;
        self.record_volume(price, amount, received_ns);

        if self.window.len() == quant_core::ANOMALY_WINDOW_TICKS {
            self.window.pop_front();
        }
        self.window.push_back(price);

        let prices: Vec<Decimal> = self.window.iter().copied().collect();
        let low = prices.iter().copied().min();
        let high = prices.iter().copied().max();
        self.snapshot.window_low = low;
        self.snapshot.window_high = high;
        self.snapshot.move_pct = match (low, high) {
            (Some(low), Some(high)) if low > Decimal::ZERO => {
                Some((high - low) / low * Decimal::from(100))
            }
            _ => None,
        };

        let anomaly = quant_core::detect_anomaly(&prices);
        self.snapshot.anomaly = anomaly;

        if !anomaly {
            return None;
        }

        // Spot is long-only: a pump is not an entry. Buying strength at the
        // top of a >1% move is exactly the trade the old directional signal
        // used to take, and on spot it can only end in a losing round trip.
        // Only downside dislocations arm an entry.
        let first = self.window.front().copied().unwrap_or(price);
        if price >= first {
            return None;
        }

        // A quiet drift down is not panic selling: without a volume spike the
        // rebound has no fuel and the spread/fees eat the trade. Do not arm the
        // cooldown either, so a real spike moments later can still signal.
        if !self.snapshot.volume_spike {
            return None;
        }

        if self
            .last_anomaly
            .is_some_and(|at| at.elapsed() < ANOMALY_COOLDOWN)
        {
            return None;
        }
        // A pump is ignored above, so it must not arm the cooldown either: the
        // next real dip is allowed to signal immediately.
        self.last_anomaly = Some(Instant::now());

        Some((
            Side::Buy,
            price,
            self.snapshot.move_pct.unwrap_or_default().round_dp(3),
            format!(
                ">{}% dip across {} ticks with 1m volume {} USDT (spike; baseline {})",
                quant_core::ANOMALY_THRESHOLD_PCT,
                prices.len(),
                self.snapshot.volume_1m.round_dp(0),
                self.snapshot.volume_baseline_1m.round_dp(0)
            ),
        ))
    }

    /// Recomputes the rolling quote volume and the volume-spike verdict.
    ///
    /// The baseline is the larger of the previous minute's flow and the 24h
    /// average minute (fed from scanner metadata). That makes the filter useful
    /// from the first tick without being fooled by a single quiet minute.
    fn record_volume(&mut self, price: Decimal, amount: Decimal, received_ns: u128) {
        let quote = price * amount;
        if quote <= Decimal::ZERO {
            self.snapshot.volume_spike = false;
            return;
        }

        let at_ns = if received_ns == 0 {
            self.volume.back().map(|sample| sample.at_ns).unwrap_or(0)
        } else {
            received_ns
        };
        self.volume.push_back(VolumeSample { at_ns, quote });
        if self.volume.len() > VOLUME_SAMPLES_CAP {
            self.volume.pop_front();
        }

        let latest = at_ns.max(
            self.volume
                .back()
                .map(|sample| sample.at_ns)
                .unwrap_or_default(),
        );
        let cutoff = latest.saturating_sub(VOLUME_WINDOW_NS);
        while self
            .volume
            .front()
            .is_some_and(|sample| sample.at_ns < cutoff)
        {
            self.volume.pop_front();
        }

        let recent_cutoff = latest.saturating_sub(VOLUME_RECENT_NS);
        let mut recent = Decimal::ZERO;
        let mut previous = Decimal::ZERO;
        for sample in &self.volume {
            if sample.at_ns >= recent_cutoff {
                recent += sample.quote;
            } else {
                previous += sample.quote;
            }
        }

        let baseline = if previous > Decimal::ZERO {
            previous.max(self.expected_minute_volume)
        } else {
            self.expected_minute_volume
        };
        let spike = if baseline > Decimal::ZERO {
            recent >= baseline * VOLUME_SPIKE_MULTIPLIER
        } else {
            // With no baseline at all (a pinned pair outside the scanner), any
            // real flow is the best spike evidence available.
            recent > Decimal::ZERO
        };

        self.snapshot.volume_1m = recent;
        self.snapshot.volume_baseline_1m = baseline;
        self.snapshot.volume_spike = recent > Decimal::ZERO && spike;
    }

    fn set_expected_minute_volume(&mut self, volume: Decimal) {
        self.expected_minute_volume = volume;
    }
}

/// The watchlist plus one rolling book per streamed symbol.
#[derive(Debug, Default)]
pub struct MarketBoard {
    pairs: Vec<PairInfo>,
    active: HashSet<String>,
    books: HashMap<String, Book>,
    /// Venue steps kept for every tracked symbol, including a pinned one after
    /// the scanner rotated it out.
    meta: HashMap<String, (Decimal, Decimal)>,
    /// Normal one-minute volume from the scanner's 24h figure.
    expected_minute_volume: HashMap<String, Decimal>,
    pinned: Option<String>,
}

impl MarketBoard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pairs(&self) -> &[PairInfo] {
        &self.pairs
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Replaces the scanner watchlist. Books for symbols that left the list are
    /// pruned unless the symbol is pinned by an open position.
    pub fn set_pairs(&mut self, pairs: Vec<PairInfo>) {
        let mut active = HashSet::new();
        for pair in &pairs {
            active.insert(pair.symbol.clone());
            self.meta
                .insert(pair.symbol.clone(), (pair.base_step, pair.price_step));
            let expected = pair.quote_volume / Decimal::from(MINUTES_PER_DAY);
            self.expected_minute_volume
                .insert(pair.symbol.clone(), expected);
            if let Some(book) = self.books.get_mut(&pair.symbol) {
                book.set_expected_minute_volume(expected);
            }
        }
        self.active = active;
        self.pairs = pairs;
        self.prune();
    }

    /// Pins the symbol of the open position so it keeps streaming until flat.
    pub fn pin(&mut self, symbol: Option<String>) {
        self.pinned = symbol;
        self.prune();
    }

    /// The full WebSocket watchlist: the scanner's picks plus a pinned position.
    pub fn subscriptions(&self) -> Vec<String> {
        let mut symbols: Vec<String> = self.active.iter().cloned().collect();
        if let Some(pinned) = &self.pinned {
            if !self.active.contains(pinned) {
                symbols.push(pinned.clone());
            }
        }
        symbols.sort();
        symbols
    }

    fn prune(&mut self) {
        let active = &self.active;
        let pinned = self.pinned.as_deref();
        let keep = |symbol: &String| active.contains(symbol) || pinned == Some(symbol.as_str());
        self.books.retain(|symbol, _| keep(symbol));
        self.meta.retain(|symbol, _| keep(symbol));
        self.expected_minute_volume.retain(|symbol, _| keep(symbol));
    }

    /// Feeds one event into the matching book. Returns a signal only when the
    /// symbol is on the active watchlist; a pinned-but-rotated-out position
    /// still gets its book updated so exits keep a live price.
    pub fn apply(&mut self, event: &MarketEvent) -> Option<Signal> {
        let symbol = event.symbol();
        let eligible = self.active.contains(symbol);
        if !eligible && self.pinned.as_deref() != Some(symbol) {
            return None;
        }

        let book = self
            .books
            .entry(symbol.to_string())
            .or_insert_with(|| Book::new(symbol, Decimal::ZERO));
        if let Some(expected) = self.expected_minute_volume.get(symbol) {
            book.set_expected_minute_volume(*expected);
        }
        let draft = book.on_event(event);

        if !eligible {
            return None;
        }
        draft.map(|(side, price, move_pct, reason)| Signal {
            symbol: symbol.to_string(),
            side,
            price,
            move_pct,
            volume_spike: self
                .books
                .get(symbol)
                .is_some_and(|book| book.snapshot.volume_spike),
            reason,
        })
    }

    pub fn snapshot(&self, symbol: &str) -> Option<MarketSnapshot> {
        self.books.get(symbol).map(|book| book.snapshot.clone())
    }

    /// Venue steps for `symbol`, if the scanner knows them.
    pub fn steps(&self, symbol: &str) -> Option<(Decimal, Decimal)> {
        self.meta.get(symbol).copied()
    }

    /// Stores venue steps for a recovered position that is not on the scanner
    /// watchlist. Call after [`MarketBoard::pin`] so the later `prune` keeps it.
    pub fn set_steps(&mut self, symbol: &str, base_step: Decimal, price_step: Decimal) {
        self.meta
            .insert(symbol.to_string(), (base_step, price_step));
    }

    pub fn is_active(&self, symbol: &str) -> bool {
        self.active.contains(symbol)
    }

    /// The multi-pair context sent to DeepSeek once a minute.
    pub fn describe_pairs(&self) -> String {
        if self.pairs.is_empty() {
            return "watchlist: empty".to_string();
        }

        let mut lines = Vec::with_capacity(self.pairs.len());
        for (index, pair) in self.pairs.iter().enumerate() {
            let book = self.books.get(&pair.symbol).map(|book| &book.snapshot);
            let last = book
                .and_then(|snapshot| snapshot.last_price)
                .unwrap_or(pair.last);
            let spread = book
                .and_then(MarketSnapshot::spread_bps)
                .or(pair.spread_bps)
                .map(|value| format!("{}bps", value.round_dp(1).normalize()))
                .unwrap_or_else(|| "-".to_string());
            let anomaly = book.is_some_and(|snapshot| snapshot.anomaly);
            let volume_1m = book.map_or(Decimal::ZERO, |snapshot| snapshot.volume_1m);
            let volume_spike = book.is_some_and(|snapshot| snapshot.volume_spike);
            let bid = book
                .and_then(|snapshot| snapshot.best_bid)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            let ask = book
                .and_then(|snapshot| snapshot.best_ask)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());

            lines.push(format!(
                "{}. {} last={} bid={} ask={} spread={} chg24={}% amp24={}% vol24={} vol1m={} volume_spike: {} anomaly={}",
                index + 1,
                pair.symbol.to_uppercase(),
                last,
                bid,
                ask,
                spread,
                pair.change_pct.round_dp(2),
                pair.amplitude_pct.round_dp(2),
                pair.quote_volume.round_dp(0),
                volume_1m.round_dp(0),
                if volume_spike { "yes" } else { "no" },
                if anomaly { "yes" } else { "no" }
            ));
        }
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn trade(symbol: &str, price: &str) -> MarketEvent {
        trade_with_amount(symbol, price, "100")
    }

    fn trade_with_amount(symbol: &str, price: &str, amount: &str) -> MarketEvent {
        MarketEvent::Trade {
            symbol: symbol.to_string(),
            price: dec(price),
            amount: dec(amount),
            exchange_ts_ms: 0,
            received_ns: 1,
        }
    }

    fn depth(symbol: &str, bid: &str, ask: &str) -> MarketEvent {
        MarketEvent::Depth {
            symbol: symbol.to_string(),
            best_bid: Some(dec(bid)),
            best_ask: Some(dec(ask)),
            exchange_ts_ms: 0,
            received_ns: 2,
        }
    }

    fn pair(symbol: &str, amplitude: &str) -> PairInfo {
        PairInfo {
            symbol: symbol.to_string(),
            last: dec("100"),
            change_pct: dec("5"),
            amplitude_pct: dec(amplitude),
            quote_volume: dec("5000000"),
            spread_bps: None,
            base_step: dec("0.000001"),
            price_step: dec("0.01"),
        }
    }

    fn board() -> MarketBoard {
        let mut board = MarketBoard::new();
        board.set_pairs(vec![pair("btcusdt", "1.5"), pair("listausdt", "20")]);
        board
    }

    #[test]
    fn depth_updates_the_book_and_spread() {
        let mut board = board();
        assert!(board.apply(&depth("btcusdt", "100", "100.1")).is_none());

        let snapshot = board.snapshot("btcusdt").expect("book must exist");
        assert_eq!(snapshot.best_bid, Some(dec("100")));
        assert_eq!(snapshot.best_ask, Some(dec("100.1")));
        assert_eq!(snapshot.spread_bps(), Some(dec("10")));
        assert_eq!(snapshot.mark_price(), Some(dec("100.05")));
    }

    #[test]
    fn a_pump_does_not_emit_an_entry_signal() {
        let mut board = board();
        assert!(board.apply(&trade("listausdt", "100")).is_none());
        assert!(
            board.apply(&trade("listausdt", "101.5")).is_none(),
            "buying a >1% pump on spot is not a long-only entry"
        );
        assert!(board.snapshot("listausdt").unwrap().anomaly);
    }

    #[test]
    fn a_dip_emits_a_buy_signal_for_that_symbol() {
        let mut board = board();
        board.apply(&trade("btcusdt", "100"));
        let signal = board
            .apply(&trade("btcusdt", "98.5"))
            .expect("a 1.5% drop must produce a dip entry");

        assert_eq!(signal.symbol, "btcusdt");
        assert_eq!(signal.side, Side::Buy);
        assert_eq!(signal.price, dec("98.5"));
        assert_eq!(signal.move_pct, dec("1.523"));
        assert!(signal.volume_spike);
        assert!(signal.reason.contains("dip"));
        assert!(signal.reason.contains("volume"));
    }

    #[test]
    fn a_quiet_dip_without_volume_is_ignored() {
        let mut board = board();
        board.apply(&trade_with_amount("btcusdt", "100", "0.001"));
        assert!(
            board
                .apply(&trade_with_amount("btcusdt", "98.5", "0.001"))
                .is_none(),
            "a quiet drift down is not panic selling"
        );
        let snapshot = board.snapshot("btcusdt").expect("book must exist");
        assert!(snapshot.anomaly);
        assert!(!snapshot.volume_spike);
    }

    #[test]
    fn a_pump_does_not_block_the_next_dip() {
        let mut board = board();
        board.apply(&trade("btcusdt", "100"));
        assert!(board.apply(&trade("btcusdt", "101.5")).is_none());

        let signal = board
            .apply(&trade("btcusdt", "98.5"))
            .expect("the ignored pump must not arm the dip cooldown");
        assert_eq!(signal.side, Side::Buy);
    }

    #[test]
    fn books_are_isolated_per_symbol() {
        let mut board = board();
        board.apply(&trade("btcusdt", "100"));
        board.apply(&trade("listausdt", "1"));

        let btc = board.snapshot("btcusdt").expect("btc book");
        let lista = board.snapshot("listausdt").expect("lista book");
        assert_eq!(btc.last_price, Some(dec("100")));
        assert_eq!(lista.last_price, Some(dec("1")));
    }

    #[test]
    fn cooldown_suppresses_back_to_back_dip_signals() {
        let mut board = board();
        board.apply(&trade("btcusdt", "100"));
        assert!(board.apply(&trade("btcusdt", "98.5")).is_some());
        assert!(board.apply(&trade("btcusdt", "97")).is_none());
    }

    #[test]
    fn flat_market_never_signals() {
        let mut board = board();
        for price in ["100", "100.01", "99.99", "100"] {
            assert!(board.apply(&trade("btcusdt", price)).is_none());
        }
        assert!(!board.snapshot("btcusdt").unwrap().anomaly);
    }

    #[test]
    fn symbols_outside_the_watchlist_are_ignored() {
        let mut board = board();
        assert!(board.apply(&trade("ethusdt", "100")).is_none());
        assert!(board.snapshot("ethusdt").is_none());
    }

    #[test]
    fn a_pinned_position_keeps_streaming_but_cannot_re_enter() {
        let mut board = board();
        board.pin(Some("btcusdt".into()));
        board.set_pairs(vec![pair("listausdt", "20")]);

        // The scanner rotated BTC out, but the open position pins it.
        assert!(board.subscriptions().contains(&"btcusdt".to_string()));
        assert!(board.apply(&trade("btcusdt", "100")).is_none());
        assert!(board.apply(&trade("btcusdt", "102")).is_none());
        assert_eq!(
            board.snapshot("btcusdt").unwrap().last_price,
            Some(dec("102"))
        );

        // Once flat, the pin is released and the book is pruned.
        board.pin(None);
        assert!(!board.subscriptions().contains(&"btcusdt".to_string()));
        assert!(board.snapshot("btcusdt").is_none());
    }

    #[test]
    fn scanner_steps_are_kept_per_symbol() {
        let mut board = MarketBoard::new();
        let mut pair = pair("btcusdt", "1.5");
        pair.base_step = dec("0.00001");
        pair.price_step = dec("0.1");
        board.set_pairs(vec![pair]);
        assert_eq!(board.steps("btcusdt"), Some((dec("0.00001"), dec("0.1"))));
    }

    #[test]
    fn describe_pairs_includes_volatility_and_live_spread() {
        let mut board = board();
        board.apply(&trade("listausdt", "0.1"));
        board.apply(&depth("listausdt", "0.1", "0.1001"));

        let text = board.describe_pairs();
        assert!(text.contains("LISTAUSDT"));
        assert!(text.contains("amp24=20%"));
        assert!(text.contains("spread=10bps"));
        assert!(text.contains("vol24=5000000"));
        assert!(text.contains("vol1m=10"));
        assert!(text.contains("volume_spike: no"));
    }
}
