//! Signed HTX spot REST client (API v1).
//!
//! Only the handful of endpoints the executor needs:
//!
//! | Method | Path | Purpose |
//! | --- | --- | --- |
//! | `GET` | `/v1/account/accounts` | spot account id (cached) |
//! | `GET` | `/v1/account/accounts/{id}/balance` | balances |
//! | `POST` | `/v1/order/orders/place` | new limit order |
//! | `GET` | `/v1/order/orders/{id}` | order state |
//! | `POST` | `/v1/order/orders/{id}/cancel` | cancel |
//!
//! Requests are signed with [`crate::execution::signing`]. Response parsing is
//! split into pure functions so the wire format is covered by unit tests without
//! touching the network.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::execution::signing::{self, SIGNATURE_METHOD, SIGNATURE_VERSION};
use crate::execution::{ExecutionError, OrderReport, OrderRequest, OrderStatus};

pub const DEFAULT_REST_URL: &str = "https://api.htx.com";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";
/// HTX rejects POSTs whose media type carries a charset suffix.
const JSON_CONTENT_TYPE: &str = "application/json";

/// One currency on the account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    pub currency: String,
    pub available: Decimal,
    pub frozen: Decimal,
}

/// A signed REST client bound to one account.
pub struct HtxRest {
    http: reqwest::Client,
    base_url: String,
    host: String,
    access_key: String,
    secret_key: String,
    account_id: OnceCell<i64>,
}

impl std::fmt::Debug for HtxRest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HtxRest")
            .field("base_url", &self.base_url)
            .field("host", &self.host)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl HtxRest {
    /// Builds a client. The URL must be `https://` — credentials never travel
    /// over plaintext.
    pub fn new(
        base_url: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Result<Self, ExecutionError> {
        let base_url = base_url.into().trim().trim_end_matches('/').to_string();
        let parsed = reqwest::Url::parse(&base_url).map_err(|error| {
            ExecutionError::Config(format!("invalid HTX_REST_URL {base_url:?}: {error}"))
        })?;
        if parsed.scheme() != "https" {
            return Err(ExecutionError::Config(format!(
                "HTX_REST_URL must use https, got {base_url:?}"
            )));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| {
                ExecutionError::Config(format!("HTX_REST_URL {base_url:?} has no host"))
            })?
            .to_string();

        let access_key = access_key.into();
        let secret_key = secret_key.into();
        if access_key.trim().is_empty() || secret_key.trim().is_empty() {
            return Err(ExecutionError::Config(
                "HTX API key and secret must not be empty".into(),
            ));
        }

        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;

        Ok(Self {
            http,
            base_url,
            host,
            access_key,
            secret_key,
            account_id: OnceCell::new(),
        })
    }

    /// Reads `HTX_API_KEY`, `HTX_API_SECRET` and the optional `HTX_REST_URL`.
    pub fn from_env() -> Result<Self, ExecutionError> {
        let access_key = std::env::var("HTX_API_KEY")
            .map_err(|_| ExecutionError::Config("HTX_API_KEY is not set".into()))?;
        let secret_key = std::env::var("HTX_API_SECRET")
            .map_err(|_| ExecutionError::Config("HTX_API_SECRET is not set".into()))?;
        let base_url =
            std::env::var("HTX_REST_URL").unwrap_or_else(|_| DEFAULT_REST_URL.to_string());
        Self::new(base_url, access_key, secret_key)
    }

    /// Signs and sends a request, then unwraps the HTX status envelope.
    async fn call(
        &self,
        method: Method,
        path: &str,
        params: BTreeMap<String, String>,
    ) -> Result<Value, ExecutionError> {
        let (url, body) = self.signed_request(
            method,
            path,
            &Utc::now().format(TIMESTAMP_FORMAT).to_string(),
            &params,
        );

        let request = match body {
            None => self.http.get(url),
            Some(body) => self
                .http
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, JSON_CONTENT_TYPE)
                .header(reqwest::header::ACCEPT, JSON_CONTENT_TYPE)
                .body(body),
        };

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        let value: Value = serde_json::from_str(&body).map_err(|error| {
            ExecutionError::Protocol(format!(
                "HTX returned non-JSON (HTTP {status}): {error}; body={}",
                truncate(&body, 300)
            ))
        })?;

        if let Some(error) = api_error(&value) {
            return Err(error);
        }
        if !status.is_success() {
            return Err(ExecutionError::Http(format!(
                "HTX HTTP {status}: {}",
                truncate(&body, 300)
            )));
        }

        Ok(value)
    }

    /// The spot account id, fetched once and cached for the process lifetime.
    pub async fn account_id(&self) -> Result<i64, ExecutionError> {
        let id = self
            .account_id
            .get_or_try_init(|| async {
                let value = self
                    .call(Method::GET, "/v1/account/accounts", BTreeMap::new())
                    .await?;
                parse_spot_account_id(&value)
            })
            .await?;
        Ok(*id)
    }

    /// Non-zero balances of the spot account.
    pub async fn spot_balances(&self) -> Result<Vec<Balance>, ExecutionError> {
        let account_id = self.account_id().await?;
        let path = format!("/v1/account/accounts/{account_id}/balance");
        let value = self.call(Method::GET, &path, BTreeMap::new()).await?;
        Ok(parse_balances(&value))
    }

    /// Places a limit order and returns the venue order id.
    ///
    /// `client-order-id` is always sent, which gives a human-readable handle for
    /// the order even if the HTTP response is lost.
    pub async fn place_order(&self, request: &OrderRequest) -> Result<String, ExecutionError> {
        let account_id = self.account_id().await?;
        let params = order_params(request, account_id);
        let value = self
            .call(Method::POST, "/v1/order/orders/place", params)
            .await?;

        value
            .get("data")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                ExecutionError::Protocol(format!(
                    "HTX place-order response has no order id: {}",
                    truncate(&value.to_string(), 300)
                ))
            })
    }

    /// Current state of an order.
    pub async fn order(&self, order_id: &str) -> Result<OrderReport, ExecutionError> {
        let path = format!("/v1/order/orders/{order_id}");
        let value = self.call(Method::GET, &path, BTreeMap::new()).await?;
        parse_order(&value).ok_or_else(|| {
            ExecutionError::Protocol(format!(
                "HTX order response is incomplete: {}",
                truncate(&value.to_string(), 300)
            ))
        })
    }

    /// Cancels an order. Cancelling an already-terminal order is reported as an
    /// API error by HTX; callers treat it as "nothing left to cancel".
    pub async fn cancel_order(&self, order_id: &str) -> Result<(), ExecutionError> {
        let path = format!("/v1/order/orders/{order_id}/cancel");
        self.call(Method::POST, &path, BTreeMap::new()).await?;
        Ok(())
    }

    /// Builds the signed URL and, for POST, the JSON body.
    ///
    /// HTX signs only the authentication fields on POST; order parameters live
    /// in an `application/json` body. GET keeps every parameter in the signed
    /// query string.
    fn signed_request(
        &self,
        method: Method,
        path: &str,
        timestamp: &str,
        params: &BTreeMap<String, String>,
    ) -> (String, Option<String>) {
        let mut auth = BTreeMap::new();
        auth.insert("AccessKeyId".into(), self.access_key.clone());
        auth.insert("SignatureMethod".into(), SIGNATURE_METHOD.into());
        auth.insert("SignatureVersion".into(), SIGNATURE_VERSION.into());
        auth.insert("Timestamp".into(), timestamp.to_string());

        let mut query = if method == Method::GET {
            params.clone()
        } else {
            BTreeMap::new()
        };
        query.extend(auth);

        let signature = signing::sign(&self.secret_key, method.as_str(), &self.host, path, &query);
        query.insert("Signature".into(), signature);

        let url = format!(
            "{}{}?{}",
            self.base_url,
            path,
            signing::canonical_query(&query)
        );
        let body = if method == Method::GET {
            None
        } else {
            Some(
                serde_json::to_string(params)
                    .expect("a map of strings is always serialisable as JSON"),
            )
        };
        (url, body)
    }
}

fn order_params(request: &OrderRequest, account_id: i64) -> BTreeMap<String, String> {
    let mut params = BTreeMap::new();
    params.insert("account-id".into(), account_id.to_string());
    params.insert("amount".into(), decimal_param(request.amount));
    params.insert("client-order-id".into(), request.client_order_id.clone());
    params.insert("price".into(), decimal_param(request.price));
    params.insert("source".into(), "api".into());
    params.insert("symbol".into(), request.symbol.to_ascii_lowercase());
    params.insert("type".into(), request.side.htx_order_type().into());
    params
}

/// `0.000380` -> `0.00038`; HTX rejects trailing-zero noise on some symbols.
fn decimal_param(value: Decimal) -> String {
    value.normalize().to_string()
}

fn parse_spot_account_id(value: &Value) -> Result<i64, ExecutionError> {
    let accounts = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| ExecutionError::Protocol("accounts response has no data array".into()))?;

    for account in accounts {
        if account.get("type").and_then(Value::as_str) != Some("spot") {
            continue;
        }
        if let Some(id) = account.get("id").and_then(value_as_i64) {
            return Ok(id);
        }
    }

    Err(ExecutionError::Protocol(
        "no spot account found for these credentials".into(),
    ))
}

fn parse_balances(value: &Value) -> Vec<Balance> {
    let Some(list) = value
        .get("data")
        .and_then(|data| data.get("list"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    let mut totals: BTreeMap<String, (Decimal, Decimal)> = BTreeMap::new();
    for entry in list {
        let Some(currency) = entry.get("currency").and_then(Value::as_str) else {
            continue;
        };
        let balance = entry
            .get("balance")
            .and_then(decimal_from_value)
            .unwrap_or(Decimal::ZERO);
        let slot = totals
            .entry(currency.to_ascii_uppercase())
            .or_insert((Decimal::ZERO, Decimal::ZERO));
        match entry.get("type").and_then(Value::as_str) {
            Some("trade") => slot.0 += balance,
            Some("frozen") => slot.1 += balance,
            _ => {}
        }
    }

    totals
        .into_iter()
        .map(|(currency, (available, frozen))| Balance {
            currency,
            available,
            frozen,
        })
        .collect()
}

fn parse_order(value: &Value) -> Option<OrderReport> {
    let data = value.get("data")?;
    let order_id = data.get("id").and_then(value_as_string)?;
    let state = data
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    Some(OrderReport {
        order_id,
        client_order_id: data
            .get("client-order-id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        status: OrderStatus::from_htx(state),
        filled_amount: data
            .get("filled-amount")
            .and_then(decimal_from_value)
            .unwrap_or(Decimal::ZERO),
        filled_cash: data
            .get("filled-cash-amount")
            .and_then(decimal_from_value)
            .unwrap_or(Decimal::ZERO),
    })
}

fn api_error(value: &Value) -> Option<ExecutionError> {
    if value.get("status").and_then(Value::as_str) != Some("error") {
        return None;
    }
    Some(ExecutionError::Api {
        code: value
            .get("err-code")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        message: value
            .get("err-msg")
            .and_then(Value::as_str)
            .unwrap_or("no message")
            .to_string(),
    })
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn decimal_from_value(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) if !text.is_empty() => Decimal::from_str(text).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut truncated: String = value.chars().take(max_chars).collect();
    truncated.push('…');
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    #[test]
    fn builds_the_htx_order_payload() {
        let request = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: crate::execution::Side::Buy,
            amount: dec("0.000380"),
            price: dec("65000.10"),
            client_order_id: "enrg-42".into(),
        };

        let params = order_params(&request, 12345678);
        assert_eq!(params.get("account-id").unwrap(), "12345678");
        assert_eq!(params.get("symbol").unwrap(), "btcusdt");
        assert_eq!(params.get("type").unwrap(), "buy-limit");
        assert_eq!(params.get("amount").unwrap(), "0.00038");
        assert_eq!(params.get("price").unwrap(), "65000.1");
        assert_eq!(params.get("source").unwrap(), "api");
        assert_eq!(params.get("client-order-id").unwrap(), "enrg-42");
    }

    #[test]
    fn post_sends_a_json_body_and_signs_only_auth_fields() {
        let rest = HtxRest::new("https://api.htx.com", "key", "secret").unwrap();
        let request = OrderRequest {
            symbol: "COMPUSDT".into(),
            side: crate::execution::Side::Buy,
            amount: dec("1.25"),
            price: dec("18.47"),
            client_order_id: "enrg-77".into(),
        };
        let params = order_params(&request, 12345678);

        let (url, body) = rest.signed_request(
            Method::POST,
            "/v1/order/orders/place",
            "2026-11-14T20:00:00",
            &params,
        );

        assert_eq!(JSON_CONTENT_TYPE, "application/json");
        assert!(!JSON_CONTENT_TYPE.contains("charset"));

        let query = url.split_once('?').expect("signed URL has a query").1;
        assert!(query.contains("AccessKeyId=key"));
        assert!(query.contains("SignatureMethod=HmacSHA256"));
        assert!(query.contains("SignatureVersion=2"));
        assert!(query.contains("Timestamp=2026-11-14T20%3A00%3A00"));
        assert!(query.contains("Signature="));
        // Golden vector from Python hmac/hashlib over the auth fields only.
        assert!(query.contains("Signature=rZ2k8VBT6eoNLSQfFbktub5pDP0L5dOJ3W7hMA1OzHU%3D"));
        // Order parameters belong to the JSON body, not the signed query.
        assert!(!query.contains("symbol="));
        assert!(!query.contains("amount="));
        assert!(!query.contains("account-id="));

        let body = body.expect("POST carries a body");
        let value: Value = serde_json::from_str(&body).expect("body must be JSON");
        assert_eq!(value["symbol"], "compusdt");
        assert_eq!(value["amount"], "1.25");
        assert_eq!(value["price"], "18.47");
        assert_eq!(value["client-order-id"], "enrg-77");
        assert!(value.get("AccessKeyId").is_none());
        assert!(value.get("Signature").is_none());
    }

    #[test]
    fn get_keeps_every_parameter_in_the_signed_query() {
        let rest = HtxRest::new("https://api.htx.com", "key", "secret").unwrap();
        let mut params = BTreeMap::new();
        params.insert("order-id".into(), "1234567890".into());
        params.insert("symbol".into(), "btcusdt".into());

        let (url, body) = rest.signed_request(
            Method::GET,
            "/v1/order/orders",
            "2026-11-14T20:00:00",
            &params,
        );

        let query = url.split_once('?').expect("signed URL has a query").1;
        assert!(query.contains("order-id=1234567890"));
        assert!(query.contains("symbol=btcusdt"));
        assert!(query.contains("AccessKeyId=key"));
        assert!(query.contains("Signature="));
        assert!(body.is_none());
    }

    #[test]
    fn selects_the_spot_account_and_skips_margin() {
        let payload = json!({
            "status": "ok",
            "data": [
                { "id": 1, "type": "margin", "state": "working" },
                { "id": "98765", "type": "spot", "state": "working" }
            ]
        });
        assert_eq!(parse_spot_account_id(&payload).unwrap(), 98765);
    }

    #[test]
    fn fails_when_no_spot_account_exists() {
        let payload = json!({ "status": "ok", "data": [] });
        assert!(parse_spot_account_id(&payload).is_err());
    }

    #[test]
    fn aggregates_free_and_frozen_balances() {
        let payload = json!({
            "status": "ok",
            "data": {
                "id": 98765,
                "type": "spot",
                "state": "working",
                "list": [
                    { "currency": "usdt", "type": "trade", "balance": "100.5" },
                    { "currency": "usdt", "type": "frozen", "balance": "20" },
                    { "currency": "btc", "type": "trade", "balance": "0.001" }
                ]
            }
        });

        let balances = parse_balances(&payload);
        assert_eq!(
            balances,
            vec![
                Balance {
                    currency: "BTC".into(),
                    available: dec("0.001"),
                    frozen: Decimal::ZERO,
                },
                Balance {
                    currency: "USDT".into(),
                    available: dec("100.5"),
                    frozen: dec("20"),
                },
            ]
        );
    }

    #[test]
    fn parses_a_filled_order() {
        let payload = json!({
            "status": "ok",
            "data": {
                "id": 59378,
                "symbol": "btcusdt",
                "type": "buy-limit",
                "amount": "0.001000",
                "price": "65000.000000",
                "filled-amount": "0.001000",
                "filled-cash-amount": "65.000000",
                "client-order-id": "enrg-1",
                "state": "filled"
            }
        });

        let report = parse_order(&payload).expect("order must parse");
        assert_eq!(report.order_id, "59378");
        assert_eq!(report.client_order_id, "enrg-1");
        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.filled_amount, dec("0.001"));
        assert_eq!(report.avg_price(), Some(dec("65000")));
    }

    #[test]
    fn parses_a_submitted_order_with_empty_fill_fields() {
        let payload = json!({
            "status": "ok",
            "data": {
                "id": "101",
                "state": "submitted",
                "filled-amount": "",
                "filled-cash-amount": ""
            }
        });

        let report = parse_order(&payload).expect("order must parse");
        assert_eq!(report.status, OrderStatus::Submitted);
        assert_eq!(report.filled_amount, Decimal::ZERO);
        assert_eq!(report.avg_price(), None);
    }

    #[test]
    fn detects_the_htx_error_envelope() {
        let payload = json!({
            "status": "error",
            "err-code": "invalid-argument",
            "err-msg": "invalid symbol"
        });

        match api_error(&payload) {
            Some(ExecutionError::Api { code, message }) => {
                assert_eq!(code, "invalid-argument");
                assert_eq!(message, "invalid symbol");
            }
            other => panic!("expected API error, got {other:?}"),
        }

        assert!(api_error(&json!({ "status": "ok", "data": [] })).is_none());
    }

    #[test]
    fn rejects_insecure_or_incomplete_configuration() {
        assert!(HtxRest::new("http://api.htx.com", "key", "secret").is_err());
        assert!(HtxRest::new("https://api.htx.com", "", "secret").is_err());
        assert!(HtxRest::new("https://api.htx.com", "key", "  ").is_err());
        assert!(HtxRest::new("https://api.htx.com", "key", "secret").is_ok());
    }
}
