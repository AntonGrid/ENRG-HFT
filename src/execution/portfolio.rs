//! Position and cash accounting.
//!
//! The bot is a spot scalper: buys are paid from cash, sells are paid from the
//! base asset. Average entry, realised and unrealised PnL are all
//! [`Decimal`]-exact so a long session does not drift by fractions of a cent.
//!
//! Fees are charged separately from the fill: the venue reports gross fills, and
//! the executor subtracts the fee from cash and from the daily result. That
//! keeps the maths auditable against exchange statements.

use rust_decimal::Decimal;

use crate::execution::Side;

/// A signed open position. `qty > 0` is long, `qty < 0` is short (paper only;
/// the risk guard forbids short entries on spot).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Position {
    qty: Decimal,
    avg_entry: Decimal,
    realized_pnl: Decimal,
}

impl Position {
    pub fn qty(&self) -> Decimal {
        self.qty
    }

    pub fn avg_entry(&self) -> Decimal {
        self.avg_entry
    }

    /// Gross realised PnL, before fees.
    pub fn realized_pnl(&self) -> Decimal {
        self.realized_pnl
    }

    pub fn is_flat(&self) -> bool {
        self.qty.is_zero()
    }

    /// Unrealised PnL at `mark`. Zero for a flat book.
    pub fn unrealized(&self, mark: Decimal) -> Decimal {
        if self.is_flat() {
            Decimal::ZERO
        } else {
            (mark - self.avg_entry) * self.qty
        }
    }

    /// Absolute exposure at `mark`.
    pub fn notional(&self, mark: Decimal) -> Decimal {
        (self.qty * mark).abs()
    }

    /// Applies a fill and returns the gross realised PnL it produced.
    ///
    /// Adding to a position re-weights the average entry. Reducing it realises
    /// PnL on the closed part; crossing through zero flips the position and
    /// resets the average entry to the fill price.
    pub fn apply_fill(&mut self, side: Side, qty: Decimal, price: Decimal) -> Decimal {
        if qty <= Decimal::ZERO || price <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let signed = match side {
            Side::Buy => qty,
            Side::Sell => -qty,
        };

        let mut realized = Decimal::ZERO;
        let old = self.qty;

        if old.is_zero() {
            self.qty = signed;
            self.avg_entry = price;
        } else if (old > Decimal::ZERO) == (side == Side::Buy) {
            // Adding to the same direction: weighted average entry.
            let total = old.abs() + qty;
            self.avg_entry = (self.avg_entry * old.abs() + price * qty) / total;
            self.qty = old + signed;
        } else {
            // Reducing, possibly flipping.
            let closing = old.abs().min(qty);
            realized = if old > Decimal::ZERO {
                (price - self.avg_entry) * closing
            } else {
                (self.avg_entry - price) * closing
            };

            let new_qty = old + signed;
            if new_qty.is_zero() {
                self.avg_entry = Decimal::ZERO;
            } else if (new_qty > Decimal::ZERO) != (old > Decimal::ZERO) {
                // Flipped: the remainder opened at the fill price.
                self.avg_entry = price;
            }
            self.qty = new_qty;
        }

        self.realized_pnl += realized;
        realized
    }
}

/// A locally recorded entry that the venue has accepted but not filled yet.
///
/// Live post-only orders spend time resting on the book. Without this overlay
/// the executor would look flat after `order accepted`, could place a second
/// entry on the next signal, and could not describe the pending position to the
/// AI. The overlay is rolled back if the order is cancelled/rejected and is
/// replaced by the real fill when it arrives.
#[derive(Debug, Clone)]
struct PendingEntry {
    before: Position,
    /// Notional committed to the order. Paper mode deducts it from `cash` so
    /// equity and affordability do not double-count an accepted entry; live
    /// mode leaves `cash` alone (the venue balance is authoritative) but still
    /// reports it so equity can avoid counting frozen USDT and the pending
    /// position twice.
    reserved_cash: Decimal,
    cash_deducted: bool,
}

/// Cash plus base asset plus the open [`Position`].
#[derive(Debug, Clone)]
pub struct Portfolio {
    /// Free quote currency (USDT in paper mode).
    pub cash: Decimal,
    pub position: Position,
    pub fees_paid: Decimal,
    pending_entry: Option<PendingEntry>,
}

impl Portfolio {
    pub fn new(cash: Decimal) -> Self {
        Self {
            cash,
            position: Position::default(),
            fees_paid: Decimal::ZERO,
            pending_entry: None,
        }
    }

    /// Records an accepted-but-not-yet-filled entry.
    ///
    /// The position immediately reflects the order so risk checks see the
    /// exposure and the AI context can show it. In paper mode the notional is
    /// also reserved from cash, otherwise equity would count both the cash and
    /// the pending position. The actual fill later replaces this overlay via
    /// [`Portfolio::clear_pending_entry`] + `apply_fill`.
    pub fn begin_pending_entry(
        &mut self,
        qty: Decimal,
        price: Decimal,
        reserve_cash: bool,
    ) -> bool {
        if qty <= Decimal::ZERO || price <= Decimal::ZERO || self.pending_entry.is_some() {
            return false;
        }
        let reserved_cash = qty * price;
        if reserve_cash && reserved_cash > self.cash {
            return false;
        }
        let before = self.position.clone();
        if reserve_cash {
            self.cash -= reserved_cash;
        }
        self.position.apply_fill(Side::Buy, qty, price);
        self.pending_entry = Some(PendingEntry {
            before,
            reserved_cash,
            cash_deducted: reserve_cash,
        });
        true
    }

    /// Notional tied up by an accepted entry that has not filled yet.
    pub fn pending_cash_reserved(&self) -> Decimal {
        self.pending_entry
            .as_ref()
            .map(|pending| pending.reserved_cash)
            .unwrap_or_default()
    }

    /// Rolls a pending entry back to the exact position and cash that preceded
    /// it. This is called on cancel/reject and immediately before an actual
    /// fill is booked.
    pub fn clear_pending_entry(&mut self) {
        if let Some(pending) = self.pending_entry.take() {
            self.position = pending.before;
            if pending.cash_deducted {
                self.cash += pending.reserved_cash;
            }
        }
    }

    /// Restores a long position discovered on the venue at startup.
    ///
    /// Cash is intentionally untouched: in live mode the exchange balance is
    /// authoritative and the local cash figure is not used for sizing. Realised
    /// PnL starts at zero because the original fill history is not persisted.
    pub fn restore_long(&mut self, qty: Decimal, avg_entry: Decimal) -> bool {
        if qty <= Decimal::ZERO || avg_entry <= Decimal::ZERO {
            return false;
        }
        self.pending_entry = None;
        self.position = Position {
            qty,
            avg_entry,
            realized_pnl: Decimal::ZERO,
        };
        true
    }

    /// Drops the locally tracked position without booking cash or PnL.
    ///
    /// Used when the venue rejects a residual as untradable dust: no fill
    /// happened, so cash, fees and realised PnL must stay exactly as they were.
    pub fn forget_position(&mut self) {
        self.pending_entry = None;
        self.position = Position::default();
    }

    /// Settles a fill against cash and the position; returns gross realised PnL.
    pub fn apply_fill(&mut self, side: Side, qty: Decimal, price: Decimal) -> Decimal {
        let cash_delta = qty * price;
        match side {
            Side::Buy => self.cash -= cash_delta,
            Side::Sell => self.cash += cash_delta,
        }
        self.position.apply_fill(side, qty, price)
    }

    /// Deducts a fee from cash and records it.
    pub fn charge_fee(&mut self, fee: Decimal) {
        if fee <= Decimal::ZERO {
            return;
        }
        self.cash -= fee;
        self.fees_paid += fee;
    }

    /// Mark-to-market equity of the paper book.
    pub fn equity(&self, mark: Decimal) -> Decimal {
        self.cash + self.position.qty * mark
    }

    /// `true` when cash covers the order and its fee.
    pub fn can_afford(&self, notional: Decimal, fee: Decimal) -> bool {
        self.cash >= notional + fee
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    #[test]
    fn buying_reweights_the_average_entry() {
        let mut position = Position::default();
        position.apply_fill(Side::Buy, dec("0.01"), dec("60000"));
        position.apply_fill(Side::Buy, dec("0.01"), dec("64000"));

        assert_eq!(position.qty(), dec("0.02"));
        assert_eq!(position.avg_entry(), dec("62000"));
        assert_eq!(position.realized_pnl(), Decimal::ZERO);
    }

    #[test]
    fn selling_part_of_a_long_realises_pnl() {
        let mut position = Position::default();
        position.apply_fill(Side::Buy, dec("0.02"), dec("62000"));
        let realized = position.apply_fill(Side::Sell, dec("0.01"), dec("66000"));

        assert_eq!(realized, dec("40"));
        assert_eq!(position.qty(), dec("0.01"));
        assert_eq!(position.avg_entry(), dec("62000"));
        assert_eq!(position.realized_pnl(), dec("40"));
    }

    #[test]
    fn closing_a_long_clears_the_average_entry() {
        let mut position = Position::default();
        position.apply_fill(Side::Buy, dec("0.01"), dec("60000"));
        position.apply_fill(Side::Sell, dec("0.01"), dec("61000"));

        assert!(position.is_flat());
        assert_eq!(position.avg_entry(), Decimal::ZERO);
        assert_eq!(position.realized_pnl(), dec("10"));
    }

    #[test]
    fn short_position_realises_on_the_way_down() {
        let mut position = Position::default();
        position.apply_fill(Side::Sell, dec("0.01"), dec("65000"));
        let realized = position.apply_fill(Side::Buy, dec("0.01"), dec("64000"));

        assert_eq!(realized, dec("10"));
        assert!(position.is_flat());
    }

    #[test]
    fn flipping_through_zero_resets_the_entry_price() {
        let mut position = Position::default();
        position.apply_fill(Side::Buy, dec("0.01"), dec("60000"));
        position.apply_fill(Side::Sell, dec("0.03"), dec("61000"));

        assert_eq!(position.qty(), dec("-0.02"));
        assert_eq!(position.avg_entry(), dec("61000"));
        assert_eq!(position.realized_pnl(), dec("10"));
    }

    #[test]
    fn unrealised_pnl_tracks_the_mark() {
        let mut position = Position::default();
        position.apply_fill(Side::Buy, dec("0.5"), dec("100"));
        assert_eq!(position.unrealized(dec("110")), dec("5"));
        assert_eq!(position.unrealized(dec("90")), dec("-5"));
        assert_eq!(position.notional(dec("110")), dec("55"));
    }

    #[test]
    fn portfolio_moves_cash_and_charges_fees() {
        let mut portfolio = Portfolio::new(dec("1000"));
        portfolio.apply_fill(Side::Buy, dec("0.001"), dec("60000"));
        assert_eq!(portfolio.cash, dec("940"));

        portfolio.apply_fill(Side::Sell, dec("0.001"), dec("61000"));
        assert_eq!(portfolio.cash, dec("1001"));

        portfolio.charge_fee(dec("0.5"));
        assert_eq!(portfolio.cash, dec("1000.5"));
        assert_eq!(portfolio.fees_paid, dec("0.5"));
        assert_eq!(portfolio.equity(dec("61000")), dec("1000.5"));
    }

    #[test]
    fn affordability_includes_the_fee() {
        let portfolio = Portfolio::new(dec("100"));
        assert!(portfolio.can_afford(dec("99"), dec("1")));
        assert!(!portfolio.can_afford(dec("99"), dec("2")));
    }

    #[test]
    fn restoring_a_long_leaves_cash_untouched() {
        let mut portfolio = Portfolio::new(dec("2"));
        assert!(portfolio.restore_long(dec("50"), dec("0.04")));

        assert_eq!(portfolio.cash, dec("2"));
        assert_eq!(portfolio.position.qty(), dec("50"));
        assert_eq!(portfolio.position.avg_entry(), dec("0.04"));
        assert_eq!(portfolio.position.realized_pnl(), Decimal::ZERO);
        assert_eq!(portfolio.position.unrealized(dec("0.05")), dec("0.5"));
    }

    #[test]
    fn restoring_a_long_rejects_empty_or_unpriced_positions() {
        let mut portfolio = Portfolio::new(dec("2"));
        assert!(!portfolio.restore_long(Decimal::ZERO, dec("1")));
        assert!(!portfolio.restore_long(dec("1"), Decimal::ZERO));
        assert!(portfolio.position.is_flat());
    }

    #[test]
    fn a_pending_entry_reserves_cash_and_updates_the_position() {
        let mut portfolio = Portfolio::new(dec("1000"));
        assert!(portfolio.begin_pending_entry(dec("0.001"), dec("60000"), true));

        assert_eq!(portfolio.cash, dec("940"), "paper cash is reserved upfront");
        assert_eq!(portfolio.position.qty(), dec("0.001"));
        assert_eq!(portfolio.position.avg_entry(), dec("60000"));
        assert!(portfolio.pending_entry.is_some());

        portfolio.clear_pending_entry();
        assert!(portfolio.position.is_flat());
        assert_eq!(portfolio.cash, dec("1000"));
        assert!(portfolio.pending_entry.is_none());
    }

    #[test]
    fn a_live_pending_entry_leaves_the_local_cash_untouched() {
        let mut portfolio = Portfolio::new(dec("0"));
        assert!(portfolio.begin_pending_entry(dec("0.001"), dec("60000"), false));

        assert_eq!(portfolio.cash, Decimal::ZERO);
        assert_eq!(portfolio.position.qty(), dec("0.001"));
        assert_eq!(
            portfolio.pending_cash_reserved(),
            dec("60"),
            "live equity still needs to know how much the venue froze"
        );

        portfolio.clear_pending_entry();
        assert!(portfolio.position.is_flat());
        assert_eq!(portfolio.cash, Decimal::ZERO);
        assert_eq!(portfolio.pending_cash_reserved(), Decimal::ZERO);
    }

    #[test]
    fn a_real_fill_replaces_the_pending_overlay_exactly_once() {
        let mut portfolio = Portfolio::new(dec("1000"));
        assert!(portfolio.begin_pending_entry(dec("0.002"), dec("50000"), true));
        assert_eq!(portfolio.cash, dec("900"));
        portfolio.clear_pending_entry();
        assert_eq!(portfolio.cash, dec("1000"));
        portfolio.apply_fill(Side::Buy, dec("0.002"), dec("50000"));

        assert_eq!(portfolio.cash, dec("900"));
        assert_eq!(portfolio.position.qty(), dec("0.002"));
        assert_eq!(portfolio.position.avg_entry(), dec("50000"));
    }

    #[test]
    fn forgetting_a_position_keeps_cash_and_fees() {
        let mut portfolio = Portfolio::new(dec("2"));
        assert!(portfolio.restore_long(dec("50"), dec("0.04")));
        portfolio.charge_fee(dec("0.5"));

        portfolio.forget_position();

        assert!(portfolio.position.is_flat());
        assert_eq!(portfolio.position.avg_entry(), Decimal::ZERO);
        assert_eq!(portfolio.position.realized_pnl(), Decimal::ZERO);
        assert_eq!(portfolio.cash, dec("1.5"));
        assert_eq!(portfolio.fees_paid, dec("0.5"));
    }
}
