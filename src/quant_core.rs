//! Layer 2 — the mathematical kernel.
//!
//! Pure, allocation-conscious [`Decimal`] implementations of the indicators the
//! trend strategy is built on: OHLC bars, EMA, Wilder's ATR and ADX, and a
//! rolling VWAP. There is no `f64` anywhere, so a reading can never drift
//! because of binary rounding.
//!
//! Design notes:
//!
//! * every indicator is *incremental*: one `update` per closed bar keeps O(1)
//!   state, so a book of a hundred symbols costs nanoseconds;
//! * Wilder's smoothing is used where the classic definition calls for it (ATR,
//!   ADX) because that is what charting platforms plot, so `atr(14)` here matches
//!   `atr(14)` on the same bars;
//! * the recursive form is seeded with the mean of the first `period` samples,
//!   which is the standard look-ahead-free seeding (no partial-window guesses);
//! * the primitives are deterministic and pure, so the strategy can be tested bar
//!   by bar without a live socket.

use std::collections::VecDeque;

use rust_decimal::Decimal;

const HUNDRED: Decimal = Decimal::ONE_HUNDRED;

/// One OHLCV bar. `volume` is *quote* volume (USDT), which is what VWAP needs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

impl Bar {
    /// A fresh bar seeded with its first print.
    pub fn new(price: Decimal, volume: Decimal) -> Self {
        Self {
            open: price,
            high: price,
            low: price,
            close: price,
            volume,
        }
    }

    /// Folds one more print into the bar.
    pub fn update(&mut self, price: Decimal, volume: Decimal) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += volume;
    }

    /// `(high + low + close) / 3`, the usual VWAP weight.
    pub fn typical(&self) -> Decimal {
        (self.high + self.low + self.close) / Decimal::from(3)
    }
}

/// Exponential moving average, seeded with the SMA of the first `period`
/// samples and then smoothed with `2 / (period + 1)`.
#[derive(Debug, Clone)]
pub struct Ema {
    period: usize,
    alpha: Decimal,
    seed: VecDeque<Decimal>,
    seed_sum: Decimal,
    value: Option<Decimal>,
}

impl Ema {
    pub fn new(period: usize) -> Self {
        let period = period.max(1);
        Self {
            period,
            alpha: Decimal::from(2) / Decimal::from(period as u32 + 1),
            seed: VecDeque::with_capacity(period),
            seed_sum: Decimal::ZERO,
            value: None,
        }
    }

    /// Folds in one close. Returns the EMA once `period` samples exist.
    pub fn update(&mut self, sample: Decimal) -> Option<Decimal> {
        if let Some(value) = self.value {
            let next = value + (sample - value) * self.alpha;
            self.value = Some(next);
            return Some(next);
        }

        self.seed.push_back(sample);
        self.seed_sum += sample;
        if self.seed.len() >= self.period {
            self.value = Some(self.seed_sum / Decimal::from(self.period as u32));
            self.seed.clear();
            self.seed_sum = Decimal::ZERO;
        }
        self.value
    }

    pub fn value(&self) -> Option<Decimal> {
        self.value
    }
}

/// Wilder's recursive average: the seed is the mean of the first `period`
/// samples, afterwards `value += (sample - value) / period`.
#[derive(Debug, Clone)]
pub struct Wilder {
    period: usize,
    seed_sum: Decimal,
    count: usize,
    value: Option<Decimal>,
}

impl Wilder {
    pub fn new(period: usize) -> Self {
        Self {
            period: period.max(1),
            seed_sum: Decimal::ZERO,
            count: 0,
            value: None,
        }
    }

    pub fn update(&mut self, sample: Decimal) -> Option<Decimal> {
        if let Some(value) = self.value {
            let next = value + (sample - value) / Decimal::from(self.period as u32);
            self.value = Some(next);
            return Some(next);
        }

        self.seed_sum += sample;
        self.count += 1;
        if self.count >= self.period {
            self.value = Some(self.seed_sum / Decimal::from(self.period as u32));
        }
        self.value
    }

    pub fn value(&self) -> Option<Decimal> {
        self.value
    }
}

/// Average True Range (Wilder). `true range` needs the previous close, so the
/// very first bar contributes its own high-low range.
#[derive(Debug, Clone)]
pub struct Atr {
    prev_close: Option<Decimal>,
    smoothing: Wilder,
}

impl Atr {
    pub fn new(period: usize) -> Self {
        Self {
            prev_close: None,
            smoothing: Wilder::new(period),
        }
    }

    pub fn update(&mut self, bar: &Bar) -> Option<Decimal> {
        let true_range = match self.prev_close {
            Some(close) => (bar.high - bar.low)
                .max((bar.high - close).abs())
                .max((bar.low - close).abs()),
            None => bar.high - bar.low,
        };
        self.prev_close = Some(bar.close);
        self.smoothing.update(true_range)
    }

    pub fn value(&self) -> Option<Decimal> {
        self.smoothing.value()
    }
}

/// One ADX reading: the trend strength plus both directional lines.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdxReading {
    pub adx: Decimal,
    pub plus_di: Decimal,
    pub minus_di: Decimal,
}

/// Wilder's Average Directional Index.
///
/// `+DI > -DI` says which side owns the move, `ADX` says whether there is a move
/// at all: below the threshold the market is ranging and the strategy stands
/// aside. That split is exactly what a trend filter needs.
#[derive(Debug, Clone)]
pub struct Adx {
    prev: Option<(Decimal, Decimal, Decimal)>,
    true_range: Wilder,
    plus_dm: Wilder,
    minus_dm: Wilder,
    dx: Wilder,
    plus_di: Option<Decimal>,
    minus_di: Option<Decimal>,
    adx: Option<Decimal>,
}

impl Adx {
    pub fn new(period: usize) -> Self {
        Self {
            prev: None,
            true_range: Wilder::new(period),
            plus_dm: Wilder::new(period),
            minus_dm: Wilder::new(period),
            dx: Wilder::new(period),
            plus_di: None,
            minus_di: None,
            adx: None,
        }
    }

    /// Folds in one *closed* bar. Returns a reading once Wilder's smoother has
    /// seen `2 * period` bars, which is when ADX is fully seeded.
    pub fn update(&mut self, bar: &Bar) -> Option<AdxReading> {
        let Some((prev_high, prev_low, prev_close)) = self.prev else {
            self.prev = Some((bar.high, bar.low, bar.close));
            return None;
        };
        self.prev = Some((bar.high, bar.low, bar.close));

        let up_move = bar.high - prev_high;
        let down_move = prev_low - bar.low;
        let plus_dm = if up_move > down_move && up_move > Decimal::ZERO {
            up_move
        } else {
            Decimal::ZERO
        };
        let minus_dm = if down_move > up_move && down_move > Decimal::ZERO {
            down_move
        } else {
            Decimal::ZERO
        };
        let true_range = (bar.high - bar.low)
            .max((bar.high - prev_close).abs())
            .max((bar.low - prev_close).abs());

        let smoothed_tr = self.true_range.update(true_range);
        let smoothed_plus = self.plus_dm.update(plus_dm);
        let smoothed_minus = self.minus_dm.update(minus_dm);
        let (Some(smoothed_tr), Some(smoothed_plus), Some(smoothed_minus)) =
            (smoothed_tr, smoothed_plus, smoothed_minus)
        else {
            return None;
        };
        if smoothed_tr <= Decimal::ZERO {
            return None;
        }

        let plus_di = smoothed_plus / smoothed_tr * HUNDRED;
        let minus_di = smoothed_minus / smoothed_tr * HUNDRED;
        self.plus_di = Some(plus_di);
        self.minus_di = Some(minus_di);

        let sum = plus_di + minus_di;
        let dx = if sum <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            (plus_di - minus_di).abs() / sum * HUNDRED
        };
        self.adx = self.dx.update(dx);
        self.reading()
    }

    /// The latest reading, or `None` while the index is still warming up.
    pub fn reading(&self) -> Option<AdxReading> {
        Some(AdxReading {
            adx: self.adx?,
            plus_di: self.plus_di?,
            minus_di: self.minus_di?,
        })
    }
}

/// Rolling VWAP over the last `bars` bars, weighted by quote volume.
///
/// A rolling window (rather than a session anchor) keeps the line meaningful on
/// a 24/7 market: there is no daily open to anchor to, and a 30-bar window reacts
/// to the same hours the strategy trades.
#[derive(Debug, Clone)]
pub struct Vwap {
    capacity: usize,
    window: VecDeque<(Decimal, Decimal)>,
    notional: Decimal,
    volume: Decimal,
}

impl Vwap {
    pub fn new(bars: usize) -> Self {
        Self {
            capacity: bars.max(1),
            window: VecDeque::with_capacity(bars.max(1)),
            notional: Decimal::ZERO,
            volume: Decimal::ZERO,
        }
    }

    pub fn update(&mut self, bar: &Bar) -> Option<Decimal> {
        let notional = bar.typical() * bar.volume;
        self.window.push_back((notional, bar.volume));
        self.notional += notional;
        self.volume += bar.volume;

        while self.window.len() > self.capacity {
            if let Some((notional, volume)) = self.window.pop_front() {
                self.notional -= notional;
                self.volume -= volume;
            }
        }
        self.value()
    }

    pub fn value(&self) -> Option<Decimal> {
        if self.volume > Decimal::ZERO && self.notional > Decimal::ZERO {
            Some(self.notional / self.volume)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn bar(open: &str, high: &str, low: &str, close: &str, volume: &str) -> Bar {
        Bar {
            open: dec(open),
            high: dec(high),
            low: dec(low),
            close: dec(close),
            volume: dec(volume),
        }
    }

    #[test]
    fn a_bar_accumulates_high_low_and_volume() {
        let mut bar = Bar::new(dec("100"), dec("10"));
        bar.update(dec("102"), dec("5"));
        bar.update(dec("99"), dec("7"));

        assert_eq!(bar.open, dec("100"));
        assert_eq!(bar.high, dec("102"));
        assert_eq!(bar.low, dec("99"));
        assert_eq!(bar.close, dec("99"));
        assert_eq!(bar.volume, dec("22"));
        assert_eq!(bar.typical(), dec("100"));
    }

    #[test]
    fn ema_is_seeded_with_the_sma_and_then_smoothed() {
        let mut ema = Ema::new(3);
        assert_eq!(ema.update(dec("1")), None);
        assert_eq!(ema.update(dec("2")), None);
        assert_eq!(ema.update(dec("3")), Some(dec("2")));
        // alpha = 2 / (3 + 1) = 0.5 -> 2 + (4 - 2) * 0.5 = 3
        assert_eq!(ema.update(dec("4")), Some(dec("3")));
        assert_eq!(ema.value(), Some(dec("3")));
    }

    #[test]
    fn wilder_average_keeps_a_constant_input_constant() {
        let mut wilder = Wilder::new(3);
        assert_eq!(wilder.update(dec("2")), None);
        assert_eq!(wilder.update(dec("2")), None);
        assert_eq!(wilder.update(dec("2")), Some(dec("2")));
        assert_eq!(wilder.update(dec("2")), Some(dec("2")));
    }

    #[test]
    fn atr_is_the_wilder_mean_of_true_range() {
        let mut atr = Atr::new(3);
        // A 2-wide bar that closes where it opened: TR is exactly 2 every time.
        let flat = bar("100", "101", "99", "100", "1");
        assert_eq!(atr.update(&flat), None);
        assert_eq!(atr.update(&flat), None);
        assert_eq!(atr.update(&flat), Some(dec("2")));
        assert_eq!(
            atr.update(&bar("100", "101", "99", "100", "1")),
            Some(dec("2")),
            "a constant true range keeps the ATR constant"
        );

        // A gap up widens the true range far beyond the bar's own range:
        // TR = |111 - 100| = 11, so ATR = 2 + (11 - 2) / 3 = 5.
        let value = atr
            .update(&bar("110", "111", "109", "110", "1"))
            .expect("ATR is ready");
        assert_eq!(value, dec("5"), "the gap must be folded into the ATR");
    }

    #[test]
    fn adx_reaches_100_on_a_one_way_trend() {
        let mut adx = Adx::new(5);
        let mut reading = None;
        for index in 0..30 {
            let close = Decimal::from(100 + index);
            reading = adx.update(&Bar {
                open: close - dec("0.5"),
                high: close + dec("0.5"),
                low: close - dec("0.5"),
                close,
                volume: Decimal::ONE,
            });
        }
        let reading = reading.expect("ADX must be ready after 30 bars");
        assert!(
            reading.adx >= dec("90"),
            "a monotonic trend is a maximally strong trend, got {}",
            reading.adx
        );
        assert!(reading.plus_di > reading.minus_di);
    }

    #[test]
    fn adx_collapses_in_a_two_way_chop() {
        let mut adx = Adx::new(5);
        let mut reading = None;
        for index in 0..30 {
            let close = if index % 2 == 0 {
                dec("100")
            } else {
                dec("100.3")
            };
            reading = adx.update(&Bar {
                open: close,
                high: close + dec("0.1"),
                low: close - dec("0.1"),
                close,
                volume: Decimal::ONE,
            });
        }
        let reading = reading.expect("ADX must be ready after 30 bars");
        assert!(
            reading.adx < dec("20"),
            "a flat chop must not look like a trend, got {}",
            reading.adx
        );
    }

    #[test]
    fn vwap_weights_each_bar_by_its_quote_volume() {
        let mut vwap = Vwap::new(10);
        vwap.update(&bar("100", "100", "100", "100", "1"));
        let value = vwap
            .update(&bar("200", "200", "200", "200", "3"))
            .expect("two bars are enough for a VWAP");
        // (100 * 1 + 200 * 3) / 4 = 175
        assert_eq!(value, dec("175"));
    }

    #[test]
    fn vwap_evicts_the_oldest_bars() {
        let mut vwap = Vwap::new(2);
        vwap.update(&bar("100", "100", "100", "100", "1"));
        vwap.update(&bar("100", "100", "100", "100", "1"));
        let value = vwap
            .update(&bar("300", "300", "300", "300", "1"))
            .expect("the window is full");
        assert_eq!(value, dec("200"), "only the last two bars count");
    }

    #[test]
    fn vwap_without_volume_has_no_value() {
        let mut vwap = Vwap::new(3);
        assert_eq!(vwap.update(&bar("100", "100", "100", "100", "0")), None);
    }
}
