//! Order brokers.
//!
//! [`Broker`] hides the difference between the paper venue (fills against the
//! live top of book, no keys required) and HTX itself. The executor talks to the
//! same interface in both modes, so a paper run exercises every code path except
//! the HTTP call.
//!
//! Paper fills are deliberately simple but not toothless: a limit order that
//! crosses the touch fills immediately at the touch price, and a resting order
//! fills only when the market later trades through it. That makes the paper PnL
//! sensitive to queue position and spread, which is exactly what a scalper must
//! feel before going live.

use rust_decimal::Decimal;

use crate::execution::htx_rest::{Balance, HtxRest};
use crate::execution::{ExecutionError, OrderReport, OrderRequest, OrderStatus, Side};
use crate::strategy::MarketSnapshot;

/// The paper venue.
#[derive(Debug, Default)]
pub struct PaperBroker {
    seq: u64,
    resting: Vec<RestingOrder>,
}

#[derive(Debug)]
struct RestingOrder {
    request: OrderRequest,
    report: OrderReport,
}

impl PaperBroker {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn open_orders(&self) -> usize {
        self.resting.len()
    }

    /// Places an order. Marketable limits fill at the touch; the rest rest.
    pub fn place(
        &mut self,
        request: &OrderRequest,
        market: &MarketSnapshot,
    ) -> Result<OrderReport, ExecutionError> {
        if market.best_bid.is_none() && market.best_ask.is_none() {
            return Err(ExecutionError::Protocol(
                "paper broker: no top of book, cannot price an order".into(),
            ));
        }
        if !request.symbol.is_empty()
            && !market.symbol.is_empty()
            && request.symbol != market.symbol
        {
            return Err(ExecutionError::Protocol(format!(
                "paper broker: order for {} cannot be priced against the {} book",
                request.symbol, market.symbol
            )));
        }

        self.seq += 1;
        let mut report = OrderReport {
            order_id: format!("paper-{}", self.seq),
            client_order_id: request.client_order_id.clone(),
            status: OrderStatus::Submitted,
            filled_amount: Decimal::ZERO,
            filled_cash: Decimal::ZERO,
        };

        if let Some(price) = crossing_price(request, market) {
            report.status = OrderStatus::Filled;
            report.filled_amount = request.amount;
            report.filled_cash = price * request.amount;
            return Ok(report);
        }

        self.resting.push(RestingOrder {
            request: request.clone(),
            report: report.clone(),
        });
        Ok(report)
    }

    /// Re-evaluates resting orders against a new snapshot, returning the ones
    /// that just filled.
    pub fn on_market(&mut self, market: &MarketSnapshot) -> Vec<OrderReport> {
        let mut filled = Vec::new();
        let mut still_resting = Vec::with_capacity(self.resting.len());

        for mut resting in self.resting.drain(..) {
            // A resting order only reacts to the book of its own symbol.
            if !market.symbol.is_empty() && resting.request.symbol != market.symbol {
                still_resting.push(resting);
                continue;
            }
            if let Some(price) = crossing_price(&resting.request, market) {
                resting.report.status = OrderStatus::Filled;
                resting.report.filled_amount = resting.request.amount;
                resting.report.filled_cash = price * resting.request.amount;
                filled.push(resting.report);
            } else {
                still_resting.push(resting);
            }
        }

        self.resting = still_resting;
        filled
    }

    pub fn status(&self, report: &OrderReport) -> OrderReport {
        self.resting
            .iter()
            .find(|resting| resting.report.order_id == report.order_id)
            .map(|resting| resting.report.clone())
            .unwrap_or_else(|| report.clone())
    }

    pub fn cancel(&mut self, report: &OrderReport) {
        self.resting
            .retain(|resting| resting.report.order_id != report.order_id);
    }
}

/// The real venue.
#[derive(Debug)]
pub struct LiveBroker {
    rest: HtxRest,
}

impl LiveBroker {
    pub fn new(rest: HtxRest) -> Self {
        Self { rest }
    }
}

/// A venue-agnostic handle used by the executor.
#[derive(Debug)]
pub enum Broker {
    Paper(PaperBroker),
    Live(LiveBroker),
}

impl Broker {
    pub fn paper() -> Self {
        Self::Paper(PaperBroker::new())
    }

    pub fn live(rest: HtxRest) -> Self {
        Self::Live(LiveBroker::new(rest))
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Paper(_) => "paper",
            Self::Live(_) => "live",
        }
    }

    pub fn is_paper(&self) -> bool {
        matches!(self, Self::Paper(_))
    }

    pub async fn place(
        &mut self,
        request: &OrderRequest,
        market: &MarketSnapshot,
    ) -> Result<OrderReport, ExecutionError> {
        match self {
            Self::Paper(paper) => paper.place(request, market),
            Self::Live(live) => {
                let order_id = live.rest.place_order(request).await?;
                Ok(OrderReport {
                    order_id,
                    client_order_id: request.client_order_id.clone(),
                    status: OrderStatus::Submitted,
                    filled_amount: Decimal::ZERO,
                    filled_cash: Decimal::ZERO,
                })
            }
        }
    }

    pub async fn status(&self, report: &OrderReport) -> Result<OrderReport, ExecutionError> {
        match self {
            Self::Paper(paper) => Ok(paper.status(report)),
            Self::Live(live) => live.rest.order(&report.order_id).await,
        }
    }

    pub async fn cancel(&mut self, report: &OrderReport) -> Result<(), ExecutionError> {
        match self {
            Self::Paper(paper) => {
                paper.cancel(report);
                Ok(())
            }
            Self::Live(live) => live.rest.cancel_order(&report.order_id).await,
        }
    }

    /// Account balances. Paper mode has no venue account, so this is live-only.
    pub async fn balances(&self) -> Result<Vec<Balance>, ExecutionError> {
        match self {
            Self::Paper(_) => Err(ExecutionError::Config(
                "balances are not available in paper mode".into(),
            )),
            Self::Live(live) => live.rest.spot_balances().await,
        }
    }

    /// Paper-only: advance resting orders against a fresh snapshot.
    pub fn on_market(&mut self, market: &MarketSnapshot) -> Vec<OrderReport> {
        match self {
            Self::Paper(paper) => paper.on_market(market),
            Self::Live(_) => Vec::new(),
        }
    }
}

/// The price at which a paper order would execute right now, if any.
fn crossing_price(request: &OrderRequest, market: &MarketSnapshot) -> Option<Decimal> {
    match request.side {
        Side::Buy => market.best_ask.filter(|ask| request.price >= *ask),
        Side::Sell => market.best_bid.filter(|bid| request.price <= *bid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn market(bid: &str, ask: &str) -> MarketSnapshot {
        MarketSnapshot {
            symbol: "btcusdt".into(),
            last_price: Some(dec(ask)),
            best_bid: Some(dec(bid)),
            best_ask: Some(dec(ask)),
            ..MarketSnapshot::default()
        }
    }

    fn request(side: Side, amount: &str, price: &str) -> OrderRequest {
        OrderRequest {
            symbol: "btcusdt".into(),
            side,
            amount: dec(amount),
            price: dec(price),
            client_order_id: "test".into(),
        }
    }

    #[test]
    fn marketable_buy_fills_at_the_ask() {
        let mut paper = PaperBroker::new();
        let report = paper
            .place(
                &request(Side::Buy, "0.001", "65001"),
                &market("65000", "65001"),
            )
            .expect("order must be accepted");

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_amount, dec("0.001"));
        assert_eq!(report.filled_cash, dec("65.001"));
        assert_eq!(report.avg_price(), Some(dec("65001")));
        assert_eq!(paper.open_orders(), 0);
    }

    #[test]
    fn passive_buy_rests_then_fills_when_the_ask_drops() {
        let mut paper = PaperBroker::new();
        let report = paper
            .place(
                &request(Side::Buy, "0.001", "64000"),
                &market("65000", "65001"),
            )
            .expect("order must be accepted");
        assert_eq!(report.status, OrderStatus::Submitted);
        assert_eq!(paper.open_orders(), 1);

        // The market is still above the limit.
        assert!(paper.on_market(&market("64900", "65000")).is_empty());

        // Ask trades down through the limit: the order fills at the ask.
        let filled = paper.on_market(&market("63900", "63950"));
        assert_eq!(filled.len(), 1);
        assert_eq!(filled[0].order_id, report.order_id);
        assert_eq!(filled[0].status, OrderStatus::Filled);
        assert_eq!(filled[0].filled_cash, dec("63.95"));
        assert_eq!(paper.open_orders(), 0);
    }

    #[test]
    fn marketable_sell_fills_at_the_bid() {
        let mut paper = PaperBroker::new();
        let report = paper
            .place(
                &request(Side::Sell, "0.002", "64999"),
                &market("65000", "65001"),
            )
            .expect("order must be accepted");

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_cash, dec("130"));
    }

    #[test]
    fn cancelling_removes_a_resting_order() {
        let mut paper = PaperBroker::new();
        let report = paper
            .place(
                &request(Side::Buy, "0.001", "64000"),
                &market("65000", "65001"),
            )
            .expect("order must be accepted");
        assert_eq!(paper.open_orders(), 1);

        paper.cancel(&report);
        assert_eq!(paper.open_orders(), 0);
        assert_eq!(paper.status(&report).status, OrderStatus::Submitted);
    }

    #[test]
    fn refuses_to_price_without_a_book() {
        let mut paper = PaperBroker::new();
        let empty = MarketSnapshot::default();
        assert!(paper
            .place(&request(Side::Buy, "0.001", "65000"), &empty)
            .is_err());
    }

    #[test]
    fn resting_orders_only_react_to_their_own_symbol() {
        let mut paper = PaperBroker::new();
        let report = paper
            .place(
                &request(Side::Buy, "0.001", "64000"),
                &market("65000", "65001"),
            )
            .expect("order must be accepted");
        assert_eq!(report.status, OrderStatus::Submitted);

        let eth = MarketSnapshot {
            symbol: "ethusdt".into(),
            last_price: Some(dec("3000")),
            best_bid: Some(dec("2999")),
            best_ask: Some(dec("3000")),
            ..MarketSnapshot::default()
        };
        assert!(paper.on_market(&eth).is_empty());
        assert_eq!(paper.open_orders(), 1);

        let filled = paper.on_market(&market("63900", "63950"));
        assert_eq!(filled.len(), 1);
        assert_eq!(filled[0].order_id, report.order_id);
    }

    #[test]
    fn refuses_to_price_an_order_against_another_symbol() {
        let mut paper = PaperBroker::new();
        let eth_market = MarketSnapshot {
            symbol: "ethusdt".into(),
            best_bid: Some(dec("2999")),
            best_ask: Some(dec("3000")),
            ..MarketSnapshot::default()
        };
        assert!(paper
            .place(&request(Side::Buy, "0.001", "3001"), &eth_market)
            .is_err());
    }

    #[test]
    fn broker_names_reflect_the_mode() {
        assert_eq!(Broker::paper().name(), "paper");
        assert!(Broker::paper().is_paper());
    }
}
