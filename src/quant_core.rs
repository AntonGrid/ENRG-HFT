//! Layer 2 — the mathematics.
//!
//! Port of the anomaly idea from `agent/physics/volatility_hunter.py`: a market
//! is interesting only when the price has actually moved, and the move has to be
//! measured precisely. Everything here is [`Decimal`] arithmetic — no `f64`, no
//! rounding surprises in the hot path.

use rust_decimal::Decimal;

/// How many latest ticks form the anomaly window.
pub const ANOMALY_WINDOW_TICKS: usize = 10;

/// Percentage threshold expressed as an integer percent (1 = 1%).
pub const ANOMALY_THRESHOLD_PCT: u32 = 1;

/// Returns `true` when the price moved by more than 1% inside the last 10 ticks.
///
/// The measured quantity is the full range of the window:
///
/// ```text
/// (max(window) - min(window)) / min(window) > 0.01
/// ```
///
/// A range (rather than only `last - first`) is used deliberately: a spike that
/// prints and immediately mean-reverts is exactly the kind of dislocation the
/// scalper must see, and a net-change check would silently miss it.
///
/// The function is pure and allocation-free; callers keep the rolling window.
pub fn detect_anomaly(prices: &[Decimal]) -> bool {
    let window = last_window(prices);
    if window.len() < 2 {
        return false;
    }

    let Some(min) = window.iter().copied().min() else {
        return false;
    };
    let Some(max) = window.iter().copied().max() else {
        return false;
    };

    if min <= Decimal::ZERO {
        return false;
    }

    let move_ratio = (max - min) / min;
    move_ratio > Decimal::new(ANOMALY_THRESHOLD_PCT as i64, 2)
}

/// The newest [`ANOMALY_WINDOW_TICKS`] observations, or the whole slice when it
/// is shorter than the window.
fn last_window(prices: &[Decimal]) -> &[Decimal] {
    let start = prices.len().saturating_sub(ANOMALY_WINDOW_TICKS);
    &prices[start..]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test price must be a valid decimal")
    }

    #[test]
    fn empty_and_single_tick_are_calm() {
        assert!(!detect_anomaly(&[]));
        assert!(!detect_anomaly(&[dec("100")]));
    }

    #[test]
    fn flat_market_is_calm() {
        let prices = [dec("100"), dec("100.01"), dec("99.99"), dec("100")];
        assert!(!detect_anomaly(&prices));
    }

    #[test]
    fn exactly_one_percent_is_not_enough() {
        let prices = [dec("100"), dec("101")];
        assert!(!detect_anomaly(&prices));
    }

    #[test]
    fn more_than_one_percent_fires() {
        let prices = [dec("100"), dec("100.5"), dec("101.01")];
        assert!(detect_anomaly(&prices));
    }

    #[test]
    fn spike_and_return_still_fires() {
        let prices = [dec("100"), dec("102"), dec("100")];
        assert!(detect_anomaly(&prices));
    }

    #[test]
    fn downward_move_fires() {
        let prices = [dec("100"), dec("98.5")];
        assert!(detect_anomaly(&prices));
    }

    #[test]
    fn only_the_last_ten_ticks_are_measured() {
        // The 10% jump is outside the window: the last ten ticks are flat.
        let mut prices = vec![dec("90")];
        prices.extend(std::iter::repeat(dec("100")).take(ANOMALY_WINDOW_TICKS));
        assert!(!detect_anomaly(&prices));

        // The same jump inside the window is detected.
        let mut prices = vec![dec("100")];
        prices.extend(std::iter::repeat(dec("110")).take(ANOMALY_WINDOW_TICKS - 1));
        assert!(detect_anomaly(&prices));
    }

    #[test]
    fn non_positive_anchor_is_ignored() {
        let prices = [dec("0"), dec("1")];
        assert!(!detect_anomaly(&prices));
    }
}
