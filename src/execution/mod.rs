//! Layer 4 — the muscles.
//!
//! Everything that can turn a market opinion into an exchange order lives here:
//! request signing, the signed REST client, portfolio accounting, the pre-trade
//! risk guard, the paper/live broker and the executor that ties them together
//! with the strategy and the AI risk gate.
//!
//! The layer is deliberately isolated from the market-data path: if it stalls on
//! an HTTP call, ticks keep flowing into the strategy and the snapshot stays
//! fresh.

pub mod broker;
pub mod engine;
pub mod htx_rest;
pub mod portfolio;
pub mod risk;
pub mod signing;

use std::fmt;

use rust_decimal::Decimal;

/// Direction of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    /// HTX v1 spot order type for a limit order in this direction.
    pub fn htx_order_type(self) -> &'static str {
        match self {
            Self::Buy => "buy-limit",
            Self::Sell => "sell-limit",
        }
    }
}

/// Lifecycle of an exchange order, normalised across paper and live venues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    /// Accepted by the venue, nothing filled yet.
    Submitted,
    /// Some quantity filled, the rest still resting.
    PartialFilled,
    Filled,
    Canceled,
    Rejected,
    /// A state this build does not know about; treated as "keep polling".
    Unknown,
}

impl OrderStatus {
    /// Maps the HTX `state` field onto [`OrderStatus`].
    pub fn from_htx(state: &str) -> Self {
        match state {
            "submitted" | "created" => Self::Submitted,
            "partial-filled" => Self::PartialFilled,
            "filled" => Self::Filled,
            "canceled" | "partial-canceled" => Self::Canceled,
            "rejected" => Self::Rejected,
            _ => Self::Unknown,
        }
    }

    /// `true` when no further exchange update can arrive for this order.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::PartialFilled => "partial-filled",
            Self::Filled => "filled",
            Self::Canceled => "canceled",
            Self::Rejected => "rejected",
            Self::Unknown => "unknown",
        }
    }
}

/// A limit order. Both spot venues require the price up front, so the executor
/// always sends a limit order and makes it marketable by pricing it through the
/// touch with a small slippage allowance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: Side,
    pub amount: Decimal,
    pub price: Decimal,
    pub client_order_id: String,
}

impl OrderRequest {
    pub fn notional(&self) -> Decimal {
        self.amount * self.price
    }
}

/// The venue's view of an order at one point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderReport {
    pub order_id: String,
    pub client_order_id: String,
    pub status: OrderStatus,
    pub filled_amount: Decimal,
    /// Quote currency spent (buy) or received (sell) on the filled part.
    pub filled_cash: Decimal,
}

impl OrderReport {
    pub fn avg_price(&self) -> Option<Decimal> {
        if self.filled_amount.is_zero() {
            None
        } else {
            Some(self.filled_cash / self.filled_amount)
        }
    }
}

/// Everything that can go wrong between an intent and a fill.
#[derive(Debug)]
pub enum ExecutionError {
    Config(String),
    Http(String),
    Api { code: String, message: String },
    Protocol(String),
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(f, "config error: {message}"),
            Self::Http(message) => write!(f, "transport error: {message}"),
            Self::Api { code, message } => write!(f, "HTX API error {code}: {message}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
        }
    }
}

impl std::error::Error for ExecutionError {}

impl From<reqwest::Error> for ExecutionError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error.to_string())
    }
}

impl From<serde_json::Error> for ExecutionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Protocol(format!("invalid JSON: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_htx_order_states() {
        assert_eq!(OrderStatus::from_htx("submitted"), OrderStatus::Submitted);
        assert_eq!(
            OrderStatus::from_htx("partial-filled"),
            OrderStatus::PartialFilled
        );
        assert_eq!(OrderStatus::from_htx("filled"), OrderStatus::Filled);
        assert_eq!(OrderStatus::from_htx("canceled"), OrderStatus::Canceled);
        assert_eq!(
            OrderStatus::from_htx("partial-canceled"),
            OrderStatus::Canceled
        );
        assert_eq!(OrderStatus::from_htx("rejected"), OrderStatus::Rejected);
        assert_eq!(OrderStatus::from_htx("wat"), OrderStatus::Unknown);
    }

    #[test]
    fn terminal_states_stop_polling() {
        assert!(OrderStatus::Filled.is_terminal());
        assert!(OrderStatus::Canceled.is_terminal());
        assert!(OrderStatus::Rejected.is_terminal());
        assert!(!OrderStatus::Submitted.is_terminal());
        assert!(!OrderStatus::PartialFilled.is_terminal());
        assert!(!OrderStatus::Unknown.is_terminal());
    }

    #[test]
    fn average_price_is_cash_over_amount() {
        use std::str::FromStr;

        let report = OrderReport {
            order_id: "1".into(),
            client_order_id: "c".into(),
            status: OrderStatus::Filled,
            filled_amount: Decimal::from_str("0.002").unwrap(),
            filled_cash: Decimal::from_str("130").unwrap(),
        };
        assert_eq!(
            report.avg_price(),
            Some(Decimal::from_str("65000").unwrap())
        );
    }

    #[test]
    fn unfilled_order_has_no_average_price() {
        let report = OrderReport {
            order_id: "1".into(),
            client_order_id: "c".into(),
            status: OrderStatus::Submitted,
            filled_amount: Decimal::ZERO,
            filled_cash: Decimal::ZERO,
        };
        assert_eq!(report.avg_price(), None);
    }

    #[test]
    fn sides_map_to_htx_order_types() {
        assert_eq!(Side::Buy.htx_order_type(), "buy-limit");
        assert_eq!(Side::Sell.htx_order_type(), "sell-limit");
    }
}
