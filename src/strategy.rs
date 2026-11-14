//! Layer 2.5 — the bridge between raw ticks and execution.
//!
//! [`Strategy`] keeps the rolling price window, the current top of book and the
//! anomaly cooldown, and turns a dislocation into a typed [`Signal`] with a
//! direction. [`MarketSnapshot`] is the read-only view shared with the AI layer
//! and the executor through a `watch` channel, so no consumer ever has to touch
//! the market-data socket or block the tick loop.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use crate::exchange::htx::MarketEvent;
use crate::execution::Side;
use crate::quant_core;

/// Minimum time between two anomaly signals.
pub const ANOMALY_COOLDOWN: Duration = Duration::from_secs(5);

/// Everything the rest of the bot needs to know about the market right now.
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

    /// One-line context for the AI risk manager.
    pub fn describe(&self) -> String {
        format!(
            "symbol={} last={} bid={} ask={} spread_bps={} range_last_{}_ticks={}% anomaly={}",
            if self.symbol.is_empty() {
                "?"
            } else {
                &self.symbol
            },
            price_text(self.last_price),
            price_text(self.best_bid),
            price_text(self.best_ask),
            price_text(self.spread_bps().map(|value| value.round_dp(2))),
            quant_core::ANOMALY_WINDOW_TICKS,
            price_text(self.move_pct.map(|value| value.round_dp(3))),
            if self.anomaly { "yes" } else { "no" }
        )
    }
}

/// A dislocation worth considering. The executor decides whether to act.
#[derive(Debug, Clone)]
pub struct Signal {
    pub side: Side,
    pub price: Decimal,
    pub move_pct: Decimal,
    pub reason: String,
}

/// Stateful market watcher.
#[derive(Debug)]
pub struct Strategy {
    window: VecDeque<Decimal>,
    snapshot: MarketSnapshot,
    last_anomaly: Option<Instant>,
}

impl Strategy {
    pub fn new(symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        Self {
            snapshot: MarketSnapshot {
                symbol,
                ..MarketSnapshot::default()
            },
            window: VecDeque::with_capacity(quant_core::ANOMALY_WINDOW_TICKS),
            last_anomaly: None,
        }
    }

    pub fn snapshot(&self) -> &MarketSnapshot {
        &self.snapshot
    }

    /// Feeds one market event. Returns a [`Signal`] when a fresh dislocation
    /// fires; every event updates the snapshot.
    pub fn on_event(&mut self, event: &MarketEvent) -> Option<Signal> {
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

    fn on_trade(&mut self, price: Decimal, received_ns: u128) -> Option<Signal> {
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

        if !anomaly
            || self
                .last_anomaly
                .is_some_and(|at| at.elapsed() < ANOMALY_COOLDOWN)
        {
            return None;
        }
        self.last_anomaly = Some(Instant::now());

        // Momentum: the direction of the net move across the window.
        let first = self.window.front().copied().unwrap_or(price);
        let side = if price >= first {
            Side::Buy
        } else {
            Side::Sell
        };

        Some(Signal {
            side,
            price,
            move_pct: self.snapshot.move_pct.unwrap_or_default().round_dp(3),
            reason: format!(
                ">{}% range in the last {} ticks",
                quant_core::ANOMALY_THRESHOLD_PCT,
                prices.len()
            ),
        })
    }
}

fn price_text(price: Option<Decimal>) -> String {
    price
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn trade(price: &str) -> MarketEvent {
        MarketEvent::Trade {
            price: dec(price),
            exchange_ts_ms: 0,
            received_ns: 1,
        }
    }

    fn depth(bid: &str, ask: &str) -> MarketEvent {
        MarketEvent::Depth {
            best_bid: Some(dec(bid)),
            best_ask: Some(dec(ask)),
            exchange_ts_ms: 0,
            received_ns: 2,
        }
    }

    #[test]
    fn depth_updates_the_book_and_spread() {
        let mut strategy = Strategy::new("btcusdt");
        assert!(strategy.on_event(&depth("100", "100.1")).is_none());

        let snapshot = strategy.snapshot();
        assert_eq!(snapshot.best_bid, Some(dec("100")));
        assert_eq!(snapshot.best_ask, Some(dec("100.1")));
        assert_eq!(snapshot.spread_bps(), Some(dec("10")));
        assert_eq!(snapshot.mark_price(), Some(dec("100.05")));
    }

    #[test]
    fn one_percent_up_move_emits_a_buy_signal() {
        let mut strategy = Strategy::new("btcusdt");
        assert!(strategy.on_event(&trade("100")).is_none());
        let signal = strategy
            .on_event(&trade("101.5"))
            .expect("a 1.5% move must produce a signal");

        assert_eq!(signal.side, Side::Buy);
        assert_eq!(signal.price, dec("101.5"));
        assert_eq!(signal.move_pct, dec("1.5"));
        assert!(strategy.snapshot().anomaly);
    }

    #[test]
    fn one_percent_down_move_emits_a_sell_signal() {
        let mut strategy = Strategy::new("btcusdt");
        strategy.on_event(&trade("100"));
        let signal = strategy
            .on_event(&trade("98.5"))
            .expect("a 1.5% drop must produce a signal");

        assert_eq!(signal.side, Side::Sell);
    }

    #[test]
    fn cooldown_suppresses_back_to_back_signals() {
        let mut strategy = Strategy::new("btcusdt");
        strategy.on_event(&trade("100"));
        assert!(strategy.on_event(&trade("102")).is_some());
        assert!(strategy.on_event(&trade("104")).is_none());
    }

    #[test]
    fn flat_market_never_signals() {
        let mut strategy = Strategy::new("btcusdt");
        for price in ["100", "100.01", "99.99", "100"] {
            assert!(strategy.on_event(&trade(price)).is_none());
        }
        assert!(!strategy.snapshot().anomaly);
    }

    #[test]
    fn describe_includes_market_and_anomaly_state() {
        let mut strategy = Strategy::new("btcusdt");
        strategy.on_event(&depth("65000", "65001"));
        strategy.on_event(&trade("65000.5"));

        let text = strategy.snapshot().describe();
        assert!(text.contains("symbol=btcusdt"));
        assert!(text.contains("last=65000.5"));
        assert!(text.contains("bid=65000"));
        assert!(text.contains("ask=65001"));
        assert!(text.contains("anomaly=no"));
    }
}
