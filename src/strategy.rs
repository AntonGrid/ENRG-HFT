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
    pub reason: String,
}

/// Rolling state for one symbol.
#[derive(Debug)]
struct Book {
    window: VecDeque<Decimal>,
    snapshot: MarketSnapshot,
    last_anomaly: Option<Instant>,
}

impl Book {
    fn new(symbol: &str) -> Self {
        Self {
            window: VecDeque::with_capacity(quant_core::ANOMALY_WINDOW_TICKS),
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
                price, received_ns, ..
            } => self.on_trade(*price, *received_ns),
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
        received_ns: u128,
    ) -> Option<(Side, Decimal, Decimal, String)> {
        self.snapshot.last_price = Some(price);
        self.snapshot.updated_ns = received_ns;

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
                ">{}% dip across {} ticks (buying the rebound)",
                quant_core::ANOMALY_THRESHOLD_PCT,
                prices.len()
            ),
        ))
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
        self.active = pairs.iter().map(|pair| pair.symbol.clone()).collect();
        for pair in &pairs {
            self.meta
                .insert(pair.symbol.clone(), (pair.base_step, pair.price_step));
        }
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
            .or_insert_with(|| Book::new(symbol));
        let draft = book.on_event(event);

        if !eligible {
            return None;
        }
        draft.map(|(side, price, move_pct, reason)| Signal {
            symbol: symbol.to_string(),
            side,
            price,
            move_pct,
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
            let bid = book
                .and_then(|snapshot| snapshot.best_bid)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            let ask = book
                .and_then(|snapshot| snapshot.best_ask)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());

            lines.push(format!(
                "{}. {} last={} bid={} ask={} spread={} chg24={}% amp24={}% vol24={} anomaly={}",
                index + 1,
                pair.symbol.to_uppercase(),
                last,
                bid,
                ask,
                spread,
                pair.change_pct.round_dp(2),
                pair.amplitude_pct.round_dp(2),
                pair.quote_volume.round_dp(0),
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
        MarketEvent::Trade {
            symbol: symbol.to_string(),
            price: dec(price),
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
        assert!(signal.reason.contains("dip"));
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
        board.apply(&depth("listausdt", "0.1", "0.1001"));

        let text = board.describe_pairs();
        assert!(text.contains("LISTAUSDT"));
        assert!(text.contains("amp24=20%"));
        assert!(text.contains("spread=10bps"));
        assert!(text.contains("vol24=5000000"));
    }
}
