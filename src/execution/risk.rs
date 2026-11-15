//! Pre-trade risk control.
//!
//! Every order — entry or exit — passes through [`RiskGuard::check`] before it
//! reaches a broker. The guard is intentionally boring and absolute: hard
//! notional caps, one position at a time, a cooldown between entries and a daily
//! loss budget. When the budget is blown the guard latches and only exits are
//! allowed, so an open position can always be closed.
//!
//! Spot only: a sell entry with a flat book is rejected instead of opening a
//! short that the venue would refuse anyway.

use std::fmt;
use std::time::{Duration, Instant};

use chrono::NaiveDate;
use rust_decimal::Decimal;

use crate::execution::{OrderRequest, Side};

/// Hard limits applied to every order.
#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub min_order_notional: Decimal,
    pub max_order_notional: Decimal,
    pub max_position_notional: Decimal,
    /// Non-positive disables the daily loss budget.
    pub max_daily_loss: Decimal,
    pub max_open_orders: usize,
    /// Non-zero enforces a pause between accepted entries.
    pub cooldown: Duration,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            min_order_notional: Decimal::from(5),
            max_order_notional: Decimal::from(50),
            max_position_notional: Decimal::from(50),
            max_daily_loss: Decimal::from(20),
            max_open_orders: 1,
            cooldown: Duration::from_secs(10),
        }
    }
}

/// Why an order is being sent. Exits bypass the entry-only limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    Entry,
    Exit,
}

/// A rejected order with a reason that can go straight to the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskRejection {
    Halted(String),
    Cooldown {
        remaining_ms: u64,
    },
    TooManyOpenOrders {
        open: usize,
        max: usize,
    },
    OrderTooSmall {
        notional: Decimal,
        min: Decimal,
    },
    OrderTooLarge {
        notional: Decimal,
        max: Decimal,
    },
    PositionLimit {
        projected: Decimal,
        max: Decimal,
    },
    InsufficientFunds {
        required: Decimal,
        available: Decimal,
    },
    /// Spot cannot open a short.
    SpotNoShort,
    NoMarketPrice,
}

impl fmt::Display for RiskRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Halted(reason) => write!(f, "trading halted: {reason}"),
            Self::Cooldown { remaining_ms } => {
                write!(f, "cooldown active, {remaining_ms}ms left")
            }
            Self::TooManyOpenOrders { open, max } => {
                write!(f, "{open} open orders (max {max})")
            }
            Self::OrderTooSmall { notional, min } => {
                write!(f, "order notional {notional} below minimum {min}")
            }
            Self::OrderTooLarge { notional, max } => {
                write!(f, "order notional {notional} above maximum {max}")
            }
            Self::PositionLimit { projected, max } => {
                write!(f, "projected position {projected} exceeds limit {max}")
            }
            Self::InsufficientFunds {
                required,
                available,
            } => write!(f, "need {required}, only {available} available"),
            Self::SpotNoShort => write!(f, "spot bot cannot open a short"),
            Self::NoMarketPrice => write!(f, "no market price"),
        }
    }
}

#[derive(Debug, Clone)]
struct HaltState {
    reason: String,
    /// A loss-triggered halt is lifted on the next UTC day; a manual one is not.
    daily: bool,
}

/// Stateful risk gate.
#[derive(Debug)]
pub struct RiskGuard {
    limits: RiskLimits,
    realized_today: Decimal,
    day: NaiveDate,
    halt: Option<HaltState>,
    last_entry_at: Option<Instant>,
}

impl RiskGuard {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            realized_today: Decimal::ZERO,
            day: chrono::Utc::now().date_naive(),
            halt: None,
            last_entry_at: None,
        }
    }

    pub fn realized_today(&self) -> Decimal {
        self.realized_today
    }

    pub fn is_halted(&self) -> bool {
        self.halt.is_some()
    }

    pub fn halt_reason(&self) -> Option<&str> {
        self.halt.as_ref().map(|halt| halt.reason.as_str())
    }

    /// Latches the guard until the next UTC day (manual halt) or [`RiskGuard::roll_day`]
    /// lifts a loss-triggered halt.
    pub fn halt(&mut self, reason: impl Into<String>) {
        self.halt = Some(HaltState {
            reason: reason.into(),
            daily: false,
        });
    }

    /// Rolls the daily budget when the UTC date changes. Returns `true` when a
    /// new day started.
    pub fn roll_day(&mut self, today: NaiveDate) -> bool {
        if today <= self.day {
            return false;
        }
        self.day = today;
        self.realized_today = Decimal::ZERO;
        if self.halt.as_ref().is_some_and(|halt| halt.daily) {
            self.halt = None;
            println!("[RISK] new UTC day: daily-loss halt lifted");
        }
        true
    }

    /// Records realised PnL (net of fees); a big enough loss latches the guard.
    pub fn record_fill(&mut self, realized_delta: Decimal) {
        if realized_delta.is_zero() {
            return;
        }
        self.realized_today += realized_delta;
        if self.limits.max_daily_loss > Decimal::ZERO
            && self.realized_today <= -self.limits.max_daily_loss
            && self.halt.is_none()
        {
            let reason = format!(
                "daily loss {} reached the {} limit",
                self.realized_today, self.limits.max_daily_loss
            );
            println!("[RISK] {reason}");
            self.halt = Some(HaltState {
                reason,
                daily: true,
            });
        }
    }

    /// The gate. `position_notional` is the absolute exposure *before* this
    /// order; `available_cash` is `None` in live mode where cash is not tracked
    /// locally.
    #[allow(clippy::too_many_arguments)]
    pub fn check(
        &mut self,
        intent: Intent,
        request: &OrderRequest,
        market_price: Decimal,
        position_notional: Decimal,
        open_orders: usize,
        available_cash: Option<Decimal>,
        now: Instant,
    ) -> Result<(), RiskRejection> {
        if market_price <= Decimal::ZERO {
            return Err(RiskRejection::NoMarketPrice);
        }

        if intent == Intent::Entry {
            if let Some(halt) = &self.halt {
                return Err(RiskRejection::Halted(halt.reason.clone()));
            }
        }

        let notional = request.amount * market_price;

        if intent == Intent::Exit {
            // Closing must work even when the guard is latched, the remaining
            // position is dust, or the entry limits are exhausted.
            return Ok(());
        }

        if notional < self.limits.min_order_notional {
            return Err(RiskRejection::OrderTooSmall {
                notional,
                min: self.limits.min_order_notional,
            });
        }
        if notional > self.limits.max_order_notional {
            return Err(RiskRejection::OrderTooLarge {
                notional,
                max: self.limits.max_order_notional,
            });
        }

        if request.side == Side::Sell && position_notional.is_zero() {
            return Err(RiskRejection::SpotNoShort);
        }

        if let Some(last) = self.last_entry_at {
            let cooldown = self.limits.cooldown;
            if !cooldown.is_zero() && now.duration_since(last) < cooldown {
                let remaining = cooldown - now.duration_since(last);
                return Err(RiskRejection::Cooldown {
                    remaining_ms: remaining.as_millis() as u64,
                });
            }
        }

        if open_orders >= self.limits.max_open_orders {
            return Err(RiskRejection::TooManyOpenOrders {
                open: open_orders,
                max: self.limits.max_open_orders,
            });
        }

        let projected = position_notional + notional;
        if projected > self.limits.max_position_notional {
            return Err(RiskRejection::PositionLimit {
                projected,
                max: self.limits.max_position_notional,
            });
        }

        if let Some(cash) = available_cash {
            if cash < notional {
                return Err(RiskRejection::InsufficientFunds {
                    required: notional,
                    available: cash,
                });
            }
        }

        self.last_entry_at = Some(now);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::OrderKind;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn limits() -> RiskLimits {
        RiskLimits {
            min_order_notional: dec("5"),
            max_order_notional: dec("50"),
            max_position_notional: dec("50"),
            max_daily_loss: dec("20"),
            max_open_orders: 1,
            cooldown: Duration::from_secs(10),
        }
    }

    fn order(side: Side, amount: &str, price: &str) -> OrderRequest {
        OrderRequest {
            symbol: "btcusdt".into(),
            side,
            amount: dec(amount),
            price: dec(price),
            kind: OrderKind::MakerLimit,
            client_order_id: "test".into(),
        }
    }

    #[test]
    fn accepts_a_reasonable_entry() {
        let mut guard = RiskGuard::new(limits());
        let request = order(Side::Buy, "0.0005", "65000");
        // 0.0005 * 65000 = 32.5 USDT
        assert_eq!(
            guard.check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                0,
                Some(dec("1000")),
                Instant::now()
            ),
            Ok(())
        );
    }

    #[test]
    fn rejects_dust_and_oversized_orders() {
        let mut guard = RiskGuard::new(limits());
        let now = Instant::now();

        let dust = order(Side::Buy, "0.00001", "65000");
        assert!(matches!(
            guard.check(
                Intent::Entry,
                &dust,
                dec("65000"),
                Decimal::ZERO,
                0,
                None,
                now
            ),
            Err(RiskRejection::OrderTooSmall { .. })
        ));

        let whale = order(Side::Buy, "0.01", "65000");
        assert!(matches!(
            guard.check(
                Intent::Entry,
                &whale,
                dec("65000"),
                Decimal::ZERO,
                0,
                None,
                now
            ),
            Err(RiskRejection::OrderTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_entries_beyond_the_position_limit() {
        let mut guard = RiskGuard::new(limits());
        let request = order(Side::Buy, "0.0005", "65000");
        // Existing exposure of 30 USDT + 32.5 = 62.5 > 50.
        assert!(matches!(
            guard.check(
                Intent::Entry,
                &request,
                dec("65000"),
                dec("30"),
                0,
                None,
                Instant::now()
            ),
            Err(RiskRejection::PositionLimit { .. })
        ));
    }

    #[test]
    fn enforces_cooldown_between_entries() {
        let mut guard = RiskGuard::new(limits());
        let request = order(Side::Buy, "0.0005", "65000");
        let now = Instant::now();

        assert!(guard
            .check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                0,
                None,
                now
            )
            .is_ok());

        assert!(matches!(
            guard.check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                0,
                None,
                now + Duration::from_secs(1)
            ),
            Err(RiskRejection::Cooldown { .. })
        ));

        assert!(guard
            .check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                0,
                None,
                now + Duration::from_secs(11)
            )
            .is_ok());
    }

    #[test]
    fn rejects_a_second_order_while_one_is_open() {
        let mut guard = RiskGuard::new(limits());
        let request = order(Side::Buy, "0.0005", "65000");
        assert!(matches!(
            guard.check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                1,
                None,
                Instant::now()
            ),
            Err(RiskRejection::TooManyOpenOrders { .. })
        ));
    }

    #[test]
    fn spot_cannot_open_a_short() {
        let mut guard = RiskGuard::new(limits());
        let request = order(Side::Sell, "0.0005", "65000");
        assert_eq!(
            guard.check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                0,
                None,
                Instant::now()
            ),
            Err(RiskRejection::SpotNoShort)
        );
    }

    #[test]
    fn rejects_entries_without_cash() {
        let mut guard = RiskGuard::new(limits());
        let request = order(Side::Buy, "0.0005", "65000");
        assert!(matches!(
            guard.check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                0,
                Some(dec("10")),
                Instant::now()
            ),
            Err(RiskRejection::InsufficientFunds { .. })
        ));
    }

    #[test]
    fn daily_loss_latches_the_guard_and_blocks_entries() {
        let mut guard = RiskGuard::new(limits());
        guard.record_fill(dec("-21"));
        assert!(guard.is_halted());

        let request = order(Side::Buy, "0.0005", "65000");
        assert!(matches!(
            guard.check(
                Intent::Entry,
                &request,
                dec("65000"),
                Decimal::ZERO,
                0,
                None,
                Instant::now()
            ),
            Err(RiskRejection::Halted(_))
        ));
    }

    #[test]
    fn exits_are_allowed_even_when_halted() {
        let mut guard = RiskGuard::new(limits());
        guard.record_fill(dec("-100"));
        let request = order(Side::Sell, "0.0005", "65000");

        assert_eq!(
            guard.check(
                Intent::Exit,
                &request,
                dec("65000"),
                dec("32.5"),
                1,
                Some(Decimal::ZERO),
                Instant::now()
            ),
            Ok(())
        );
    }

    #[test]
    fn exits_are_allowed_even_for_dust() {
        let mut guard = RiskGuard::new(limits());
        // 0.00001 BTC is 0.65 USDT, far below the 5 USDT entry minimum.
        let request = order(Side::Sell, "0.00001", "65000");

        assert_eq!(
            guard.check(
                Intent::Exit,
                &request,
                dec("65000"),
                dec("0.65"),
                1,
                Some(Decimal::ZERO),
                Instant::now()
            ),
            Ok(())
        );
    }

    #[test]
    fn a_new_day_clears_a_loss_halt_but_not_a_manual_one() {
        let mut guard = RiskGuard::new(limits());
        guard.record_fill(dec("-21"));
        let tomorrow = guard.day + chrono::Days::new(1);
        assert!(guard.roll_day(tomorrow));
        assert!(!guard.is_halted());
        assert_eq!(guard.realized_today(), Decimal::ZERO);

        guard.halt("operator pressed the button");
        assert!(guard.roll_day(tomorrow + chrono::Days::new(1)));
        assert!(guard.is_halted());
    }

    #[test]
    fn non_positive_daily_limit_disables_the_halt() {
        let mut guard = RiskGuard::new(RiskLimits {
            max_daily_loss: Decimal::ZERO,
            ..limits()
        });
        guard.record_fill(dec("-1000"));
        assert!(!guard.is_halted());
    }
}
