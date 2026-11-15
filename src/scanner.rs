//! Layer 0.5 — the hunter's eyes.
//!
//! The bot no longer watches one sleepy symbol. Every [`ScannerConfig::interval`]
//! this module pulls the whole HTX spot ticker board, throws away everything that
//! is not a liquid USDT pair and ranks the survivors by 24h amplitude, so the
//! market-data layer always streams the pairs that are actually moving.
//!
//! The scanner is read-only and never trades: it only produces a ranked
//! watchlist. Trading decisions stay in the strategy, the AI gate and the risk
//! guard.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use rust_decimal::Decimal;
use serde_json::Value;

const DEFAULT_REST_URL: &str = "https://api.htx.com";
const TICKERS_PATH: &str = "/market/tickers";
const SYMBOLS_PATH: &str = "/v1/common/symbols";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const DEFAULT_INTERVAL_SECS: u64 = 90;
const DEFAULT_MIN_QUOTE_VOLUME: &str = "1000000";
/// Hard default: a scalp with a wider touch cannot survive 2 x taker fees plus
/// adverse selection, so pairs above 40 bps are never shown to the AI.
const DEFAULT_MAX_SPREAD_BPS: &str = "40";
const DEFAULT_TOP_N: usize = 12;
const MAX_TOP_N: usize = 50;
const QUOTE: &str = "usdt";

/// Stablecoin bases: quoting them is pointless, they do not move.
const STABLE_BASES: [&str; 16] = [
    "usdt", "usdc", "dai", "tusd", "usdd", "fdusd", "busd", "usde", "usds", "pyusd", "eurc",
    "usd1", "susds", "susde", "gusd", "usdp",
];

/// Leveraged-token suffixes (`btc3l`, `eth5s`, …): high amplitude, terrible
/// liquidity and a different risk profile. Not our game.
const LEVERAGED_SUFFIXES: [&str; 8] = ["2l", "2s", "3l", "3s", "4l", "4s", "5l", "5s"];

/// Everything the scanner needs, read once at boot.
#[derive(Debug, Clone)]
pub struct ScannerConfig {
    pub interval: Duration,
    /// Minimum 24h quote volume, in USDT. Illiquid pairs are dropped.
    pub min_quote_volume: Decimal,
    /// Maximum quoted spread at scan time, in basis points. A 20% spread is not
    /// a scalp, it is a donation; zero disables the filter.
    pub max_spread_bps: Decimal,
    /// How many pairs to stream.
    pub top_n: usize,
    /// Pairs that always stay on the watchlist, whatever their volume.
    pub pinned: Vec<String>,
    pub rest_url: String,
    /// Fallback venue steps for symbols missing from `/v1/common/symbols`.
    pub fallback_base_step: Decimal,
    pub fallback_price_step: Decimal,
}

impl ScannerConfig {
    pub fn from_env() -> Result<Self, ScannerError> {
        let interval_secs = env_u64("SCAN_INTERVAL_SECS", DEFAULT_INTERVAL_SECS)?;
        if interval_secs == 0 {
            return Err(ScannerError::Config(
                "SCAN_INTERVAL_SECS must be positive".into(),
            ));
        }
        let top_n = env_u64("SCAN_TOP_N", DEFAULT_TOP_N as u64)?;
        if top_n == 0 || top_n as usize > MAX_TOP_N {
            return Err(ScannerError::Config(format!(
                "SCAN_TOP_N must be between 1 and {MAX_TOP_N}"
            )));
        }
        let min_quote_volume = env_decimal("SCAN_MIN_QUOTE_VOLUME_USDT", DEFAULT_MIN_QUOTE_VOLUME)?;
        if min_quote_volume < Decimal::ZERO {
            return Err(ScannerError::Config(
                "SCAN_MIN_QUOTE_VOLUME_USDT must not be negative".into(),
            ));
        }
        let max_spread_bps = env_decimal("SCAN_MAX_SPREAD_BPS", DEFAULT_MAX_SPREAD_BPS)?;
        if max_spread_bps < Decimal::ZERO {
            return Err(ScannerError::Config(
                "SCAN_MAX_SPREAD_BPS must not be negative (0 disables the filter)".into(),
            ));
        }
        let base_step = env_decimal("EXEC_BASE_STEP", "0.000001")?;
        let price_step = env_decimal("EXEC_PRICE_STEP", "0.01")?;
        if base_step <= Decimal::ZERO || price_step <= Decimal::ZERO {
            return Err(ScannerError::Config(
                "EXEC_BASE_STEP and EXEC_PRICE_STEP must be positive".into(),
            ));
        }

        Ok(Self {
            interval: Duration::from_secs(interval_secs),
            min_quote_volume,
            max_spread_bps,
            top_n: top_n as usize,
            pinned: env_string("SCAN_SYMBOLS", "")
                .split(',')
                .filter_map(normalize_symbol)
                .collect(),
            rest_url: env_string("HTX_REST_URL", DEFAULT_REST_URL)
                .trim_end_matches('/')
                .to_string(),
            fallback_base_step: base_step,
            fallback_price_step: price_step,
        })
    }
}

/// One liquid, volatile pair selected by the scanner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairInfo {
    pub symbol: String,
    pub last: Decimal,
    /// 24h change in percent, signed.
    pub change_pct: Decimal,
    /// 24h range `(high - low) / low` in percent: the volatility ranking key.
    pub amplitude_pct: Decimal,
    /// 24h quote (USDT) volume.
    pub quote_volume: Decimal,
    /// Quoted spread at scan time, in basis points.
    pub spread_bps: Option<Decimal>,
    pub base_step: Decimal,
    pub price_step: Decimal,
}

impl PairInfo {
    /// One-line, human-readable summary for the scan log.
    pub fn describe(&self) -> String {
        format!(
            "{} last={} chg={}% amp={}% vol={} spread={}",
            self.symbol.to_uppercase(),
            self.last,
            self.change_pct.round_dp(2),
            self.amplitude_pct.round_dp(2),
            self.quote_volume.round_dp(0),
            self.spread_bps
                .map(|value| format!("{}bps", value.round_dp(0).normalize()))
                .unwrap_or_else(|| "-".to_string())
        )
    }
}

/// Venue steps for a symbol, derived from `/v1/common/symbols` precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolMeta {
    pub base_step: Decimal,
    pub price_step: Decimal,
}

/// A price quote for one spot market, used by startup position recovery.
///
/// Unlike [`PairInfo`] this keeps stablecoin and leveraged-token markets too:
/// the recovery audit has to be able to liquidate whatever the account holds,
/// not only the pairs the scanner would choose for trading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotQuote {
    pub symbol: String,
    pub last: Decimal,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub base_step: Decimal,
    pub price_step: Decimal,
}

impl SpotQuote {
    /// The price used to value a balance and anchor a recovered stop.
    pub fn mark(&self) -> Option<Decimal> {
        if self.last > Decimal::ZERO {
            return Some(self.last);
        }
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO && ask >= bid => {
                Some((bid + ask) / Decimal::TWO)
            }
            (Some(bid), _) if bid > Decimal::ZERO => Some(bid),
            (_, Some(ask)) if ask > Decimal::ZERO => Some(ask),
            _ => None,
        }
    }
}

/// A raw ticker before filtering.
#[derive(Debug, Clone)]
struct Ticker {
    symbol: String,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    quote_volume: Decimal,
    bid: Option<Decimal>,
    ask: Option<Decimal>,
}

/// The REST scanner.
#[derive(Debug)]
pub struct Scanner {
    client: reqwest::Client,
    config: ScannerConfig,
}

impl Scanner {
    pub fn new(config: ScannerConfig) -> Result<Self, ScannerError> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self { client, config })
    }

    /// Pulls the ticker board and returns the ranked watchlist.
    ///
    /// Precision metadata is best-effort: if `/v1/common/symbols` fails the
    /// scanner still returns pairs with the configured fallback steps, because
    /// losing the watchlist over metadata would be worse than coarse sizing.
    pub async fn scan(&self) -> Result<Vec<PairInfo>, ScannerError> {
        let tickers = self.fetch(TICKERS_PATH).await?;
        let tickers = parse_tickers(&tickers)?;
        let metas = self.symbol_meta().await;

        let mut pairs = rank_pairs(&tickers, &metas, &self.config);
        append_pinned(&mut pairs, &tickers, &metas, &self.config);
        Ok(pairs)
    }

    /// Every priced spot market, keyed by normalised symbol.
    ///
    /// This is deliberately broader than [`Scanner::scan`]: startup recovery
    /// must be able to value and liquidate a balance even when that pair is not
    /// volatile or liquid enough to make the trading watchlist.
    pub async fn quotes(&self) -> Result<HashMap<String, SpotQuote>, ScannerError> {
        let tickers = self.fetch(TICKERS_PATH).await?;
        let tickers = parse_tickers(&tickers)?;
        let metas = self.symbol_meta().await;

        Ok(tickers
            .iter()
            .filter_map(|ticker| spot_quote(ticker, &metas, &self.config))
            .map(|quote| (quote.symbol.clone(), quote))
            .collect())
    }

    async fn symbol_meta(&self) -> HashMap<String, SymbolMeta> {
        match self.fetch(SYMBOLS_PATH).await {
            Ok(value) => parse_symbol_meta(&value),
            Err(error) => {
                eprintln!("[SCAN] symbol metadata unavailable ({error}); using fallback steps");
                HashMap::new()
            }
        }
    }

    async fn fetch(&self, path: &str) -> Result<Value, ScannerError> {
        let url = format!("{}{}", self.config.rest_url, path);
        let response = self.client.get(&url).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(ScannerError::Http(format!("GET {url} -> HTTP {status}")));
        }
        let value: Value = serde_json::from_str(&body).map_err(|error| {
            ScannerError::Protocol(format!("GET {url} returned invalid JSON: {error}"))
        })?;
        if let Some(status) = value.get("status").and_then(Value::as_str) {
            if status != "ok" {
                let message = value
                    .get("err-msg")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error");
                return Err(ScannerError::Protocol(format!(
                    "GET {url} failed: {message}"
                )));
            }
        }
        Ok(value)
    }
}

/// Filters, ranks and truncates the ticker board.
fn rank_pairs(
    tickers: &[Ticker],
    metas: &HashMap<String, SymbolMeta>,
    config: &ScannerConfig,
) -> Vec<PairInfo> {
    let mut pairs: Vec<PairInfo> = tickers
        .iter()
        .filter_map(|ticker| pair_from_ticker(ticker, metas, config))
        .collect();

    pairs.sort_by(|a, b| {
        b.amplitude_pct
            .cmp(&a.amplitude_pct)
            .then_with(|| b.quote_volume.cmp(&a.quote_volume))
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
    pairs.truncate(config.top_n);
    pairs
}

/// Explicitly pinned pairs bypass the liquidity filter and are appended after
/// the ranked list, so an operator can always watch a specific market.
fn append_pinned(
    pairs: &mut Vec<PairInfo>,
    tickers: &[Ticker],
    metas: &HashMap<String, SymbolMeta>,
    config: &ScannerConfig,
) {
    if config.pinned.is_empty() {
        return;
    }
    let mut seen: HashSet<String> = pairs.iter().map(|pair| pair.symbol.clone()).collect();
    for symbol in &config.pinned {
        if seen.contains(symbol) {
            continue;
        }
        let Some(ticker) = tickers.iter().find(|ticker| &ticker.symbol == symbol) else {
            eprintln!("[SCAN] pinned symbol {symbol} is not on the HTX ticker board");
            continue;
        };
        if let Some(pair) = pair_from_ticker_unchecked(ticker, metas, config) {
            seen.insert(symbol.clone());
            pairs.push(pair);
        }
    }
}

fn spot_quote(
    ticker: &Ticker,
    metas: &HashMap<String, SymbolMeta>,
    config: &ScannerConfig,
) -> Option<SpotQuote> {
    if ticker.close <= Decimal::ZERO {
        return None;
    }
    let (base_step, price_step) = metas
        .get(&ticker.symbol)
        .map(|meta| (meta.base_step, meta.price_step))
        .unwrap_or((config.fallback_base_step, config.fallback_price_step));

    Some(SpotQuote {
        symbol: ticker.symbol.clone(),
        last: ticker.close,
        best_bid: ticker.bid,
        best_ask: ticker.ask,
        base_step,
        price_step,
    })
}

fn pair_from_ticker(
    ticker: &Ticker,
    metas: &HashMap<String, SymbolMeta>,
    config: &ScannerConfig,
) -> Option<PairInfo> {
    if !is_tradable_usdt_symbol(&ticker.symbol) {
        return None;
    }
    if ticker.quote_volume < config.min_quote_volume {
        return None;
    }
    let spread = quoted_spread_bps(ticker.bid, ticker.ask);
    if config.max_spread_bps > Decimal::ZERO {
        match spread {
            Some(spread) if spread > config.max_spread_bps => return None,
            // A ticker without a usable book cannot be judged; the AI sees the
            // live spread later, so do not drop it on missing data alone.
            _ => {}
        }
    }
    pair_from_ticker_unchecked(ticker, metas, config)
}

fn pair_from_ticker_unchecked(
    ticker: &Ticker,
    metas: &HashMap<String, SymbolMeta>,
    config: &ScannerConfig,
) -> Option<PairInfo> {
    if ticker.close <= Decimal::ZERO || ticker.low <= Decimal::ZERO || ticker.open <= Decimal::ZERO
    {
        return None;
    }
    let amplitude_pct = (ticker.high - ticker.low) / ticker.low * Decimal::from(100);
    if amplitude_pct <= Decimal::ZERO {
        return None;
    }
    let change_pct = (ticker.close - ticker.open) / ticker.open * Decimal::from(100);
    let (base_step, price_step) = metas
        .get(&ticker.symbol)
        .map(|meta| (meta.base_step, meta.price_step))
        .unwrap_or((config.fallback_base_step, config.fallback_price_step));

    Some(PairInfo {
        symbol: ticker.symbol.clone(),
        last: ticker.close,
        change_pct,
        amplitude_pct,
        quote_volume: ticker.quote_volume,
        spread_bps: quoted_spread_bps(ticker.bid, ticker.ask),
        base_step,
        price_step,
    })
}

/// Quoted spread in basis points, or `None` when the ticker has no usable book.
fn quoted_spread_bps(bid: Option<Decimal>, ask: Option<Decimal>) -> Option<Decimal> {
    match (bid, ask) {
        (Some(bid), Some(ask)) if bid > Decimal::ZERO && ask >= bid => {
            Some((ask - bid) / bid * Decimal::from(10_000))
        }
        _ => None,
    }
}

/// `true` for a plain, non-stable, non-leveraged `<base>usdt` pair.
pub(crate) fn is_tradable_usdt_symbol(symbol: &str) -> bool {
    let Some(base) = symbol.strip_suffix(QUOTE) else {
        return false;
    };
    if base.is_empty() || base.len() > 20 {
        return false;
    }
    if !base.chars().all(|c| c.is_ascii_alphanumeric()) {
        return false;
    }
    if STABLE_BASES.contains(&base) {
        return false;
    }
    if LEVERAGED_SUFFIXES
        .iter()
        .any(|suffix| base.ends_with(suffix))
    {
        return false;
    }
    true
}

/// Normalises a user-supplied symbol: lower case, alphanumeric only.
pub fn normalize_symbol(value: &str) -> Option<String> {
    let symbol: String = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    if symbol.is_empty() {
        None
    } else {
        Some(symbol)
    }
}

/// Parses `/market/tickers` into raw tickers, skipping malformed rows.
fn parse_tickers(value: &Value) -> Result<Vec<Ticker>, ScannerError> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| ScannerError::Protocol("ticker board has no data array".into()))?;

    let mut tickers = Vec::with_capacity(data.len());
    for row in data {
        let Some(symbol) = row.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let Some(symbol) = normalize_symbol(symbol) else {
            continue;
        };
        let Some(open) = row.get("open").and_then(decimal_from_value) else {
            continue;
        };
        let Some(high) = row.get("high").and_then(decimal_from_value) else {
            continue;
        };
        let Some(low) = row.get("low").and_then(decimal_from_value) else {
            continue;
        };
        let Some(close) = row.get("close").and_then(decimal_from_value) else {
            continue;
        };
        let Some(quote_volume) = row.get("vol").and_then(decimal_from_value) else {
            continue;
        };
        tickers.push(Ticker {
            symbol,
            open,
            high,
            low,
            close,
            quote_volume,
            bid: row.get("bid").and_then(decimal_from_value),
            ask: row.get("ask").and_then(decimal_from_value),
        });
    }
    Ok(tickers)
}

/// Parses `/v1/common/symbols` precision into venue steps.
fn parse_symbol_meta(value: &Value) -> HashMap<String, SymbolMeta> {
    let mut metas = HashMap::new();
    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return metas;
    };
    for row in rows {
        let Some(symbol) = row.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let Some(symbol) = normalize_symbol(symbol) else {
            continue;
        };
        let Some(amount_precision) = row.get("amount-precision").and_then(Value::as_u64) else {
            continue;
        };
        let Some(price_precision) = row.get("price-precision").and_then(Value::as_u64) else {
            continue;
        };
        let (Some(base_step), Some(price_step)) = (
            step_from_precision(amount_precision),
            step_from_precision(price_precision),
        ) else {
            continue;
        };
        metas.insert(
            symbol,
            SymbolMeta {
                base_step,
                price_step,
            },
        );
    }
    metas
}

/// `precision = 4` -> `0.0001`.
fn step_from_precision(precision: u64) -> Option<Decimal> {
    if precision > 18 {
        return None;
    }
    Some(Decimal::new(1, precision as u32))
}

fn decimal_from_value(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str(text).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_decimal(name: &str, default: &str) -> Result<Decimal, ScannerError> {
    let raw = env_string(name, default);
    Decimal::from_str(&raw)
        .map_err(|error| ScannerError::Config(format!("{name}={raw:?} is not a decimal: {error}")))
}

fn env_u64(name: &str, default: u64) -> Result<u64, ScannerError> {
    let raw = env_string(name, &default.to_string());
    raw.parse::<u64>()
        .map_err(|error| ScannerError::Config(format!("{name}={raw:?} is not a number: {error}")))
}

/// Everything that can go wrong while scanning.
#[derive(Debug)]
pub enum ScannerError {
    Config(String),
    Http(String),
    Protocol(String),
}

impl fmt::Display for ScannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(f, "scanner config error: {message}"),
            Self::Http(message) => write!(f, "scanner transport error: {message}"),
            Self::Protocol(message) => write!(f, "scanner protocol error: {message}"),
        }
    }
}

impl std::error::Error for ScannerError {}

impl From<reqwest::Error> for ScannerError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test value must be a valid decimal")
    }

    fn config() -> ScannerConfig {
        ScannerConfig {
            interval: Duration::from_secs(90),
            min_quote_volume: dec("1000000"),
            max_spread_bps: dec(DEFAULT_MAX_SPREAD_BPS),
            top_n: 3,
            pinned: Vec::new(),
            rest_url: DEFAULT_REST_URL.into(),
            fallback_base_step: dec("0.000001"),
            fallback_price_step: dec("0.01"),
        }
    }

    fn board() -> Value {
        json!({
            "data": [
                { "symbol": "btcusdt", "open": 60000.0, "high": 62000.0, "low": 59000.0, "close": 61000.0, "vol": 900000000.0, "bid": 60999.0, "ask": 61001.0 },
                { "symbol": "listausdt", "open": 0.10, "high": 0.14, "low": 0.10, "close": 0.13, "vol": 4200000.0, "bid": 0.1299, "ask": 0.1301 },
                { "symbol": "ethusdt", "open": 3000.0, "high": 3050.0, "low": 2990.0, "close": 3010.0, "vol": 500000000.0, "bid": 3009.0, "ask": 3011.0 },
                { "symbol": "funusdt", "open": 0.0017, "high": 0.0021, "low": 0.0016, "close": 0.0020, "vol": 2500000.0, "bid": 0.001999, "ask": 0.002001 },
                { "symbol": "wideusdt", "open": 1.0, "high": 2.0, "low": 0.8, "close": 1.5, "vol": 5000000.0, "bid": 1.0, "ask": 1.2 },
                { "symbol": "zigusdt", "open": 0.036, "high": 0.036, "low": 0.036, "close": 0.036, "vol": 0.0, "bid": 0.036, "ask": 0.08 },
                { "symbol": "shibusdt", "open": 0.00002, "high": 0.00003, "low": 0.00002, "close": 0.000025, "vol": 900000.0, "bid": 0.0000249, "ask": 0.0000251 },
                { "symbol": "usdcusdt", "open": 1.0, "high": 1.001, "low": 0.999, "close": 1.0, "vol": 80000000.0, "bid": 0.9999, "ask": 1.0001 },
                { "symbol": "btc3lusdt", "open": 10.0, "high": 15.0, "low": 9.0, "close": 14.0, "vol": 9000000.0, "bid": 13.9, "ask": 14.1 },
                { "symbol": "btcbtc", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "vol": 100000000.0, "bid": 1.0, "ask": 1.0 }
            ]
        })
    }

    fn metas() -> HashMap<String, SymbolMeta> {
        let mut metas = HashMap::new();
        metas.insert(
            "listausdt".into(),
            SymbolMeta {
                base_step: dec("1"),
                price_step: dec("0.0001"),
            },
        );
        metas
    }

    #[test]
    fn filters_illiquid_stable_and_leveraged_pairs() {
        let tickers = parse_tickers(&board()).expect("board must parse");
        let pairs = rank_pairs(&tickers, &metas(), &config());
        let symbols: Vec<&str> = pairs.iter().map(|pair| pair.symbol.as_str()).collect();

        assert_eq!(symbols, vec!["listausdt", "funusdt", "btcusdt"]);
        assert!(!symbols.contains(&"shibusdt"), "volume below $1M");
        assert!(!symbols.contains(&"usdcusdt"), "stablecoin base");
        assert!(!symbols.contains(&"btc3lusdt"), "leveraged token");
        assert!(!symbols.contains(&"zigusdt"), "no volume");
        assert!(
            !symbols.contains(&"wideusdt"),
            "a 20% quoted spread is not tradable"
        );
    }

    #[test]
    fn forty_bps_is_the_default_spread_ceiling() {
        let value = json!({
            "data": [
                { "symbol": "tightusdt", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "vol": 5000000.0, "bid": 1.0, "ask": 1.0041 },
                { "symbol": "tradesusdt", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "vol": 5000000.0, "bid": 1.0, "ask": 1.0039 }
            ]
        });
        let tickers = parse_tickers(&value).expect("board must parse");
        let pairs = rank_pairs(&tickers, &HashMap::new(), &config());
        let symbols: Vec<&str> = pairs.iter().map(|pair| pair.symbol.as_str()).collect();

        assert!(
            !symbols.contains(&"tightusdt"),
            "41 bps must be dropped by the default 40 bps ceiling"
        );
        assert!(
            symbols.contains(&"tradesusdt"),
            "39 bps is still inside the ceiling"
        );
    }

    #[test]
    fn zero_max_spread_disables_the_spread_filter() {
        let tickers = parse_tickers(&board()).expect("board must parse");
        let mut cfg = config();
        cfg.max_spread_bps = Decimal::ZERO;
        let pairs = rank_pairs(&tickers, &metas(), &cfg);
        assert_eq!(pairs[0].symbol, "wideusdt");
    }

    #[test]
    fn ranks_by_amplitude_and_keeps_precision_steps() {
        let tickers = parse_tickers(&board()).expect("board must parse");
        let pairs = rank_pairs(&tickers, &metas(), &config());

        assert_eq!(pairs[0].symbol, "listausdt");
        assert_eq!(pairs[0].amplitude_pct, dec("40"));
        assert_eq!(pairs[0].change_pct, dec("30"));
        assert_eq!(pairs[0].base_step, dec("1"));
        assert_eq!(pairs[0].price_step, dec("0.0001"));
        let spread = pairs[0].spread_bps.expect("quoted spread must be recorded");
        assert!(spread > dec("10") && spread < dec("20"));

        // funusdt is not in the metadata map: the configured fallback applies.
        assert_eq!(pairs[1].base_step, dec("0.000001"));
        assert_eq!(pairs[1].price_step, dec("0.01"));
    }

    #[test]
    fn respects_top_n() {
        let tickers = parse_tickers(&board()).expect("board must parse");
        let mut cfg = config();
        cfg.top_n = 1;
        assert_eq!(rank_pairs(&tickers, &metas(), &cfg).len(), 1);
    }

    #[test]
    fn pinned_pairs_bypass_the_liquidity_filter() {
        let tickers = parse_tickers(&board()).expect("board must parse");
        let mut cfg = config();
        cfg.pinned = vec!["shibusdt".into()];

        let mut pairs = rank_pairs(&tickers, &metas(), &cfg);
        append_pinned(&mut pairs, &tickers, &metas(), &cfg);

        let symbols: Vec<&str> = pairs.iter().map(|pair| pair.symbol.as_str()).collect();
        assert!(symbols.contains(&"shibusdt"));
        assert_eq!(pairs.len(), 4);
    }

    #[test]
    fn parses_symbol_precision() {
        let value = json!({
            "status": "ok",
            "data": [
                { "symbol": "btcusdt", "amount-precision": 6, "price-precision": 2 },
                { "symbol": "listausdt", "amount-precision": 0, "price-precision": 6 },
                { "symbol": "badsymbol" }
            ]
        });
        let metas = parse_symbol_meta(&value);

        assert_eq!(metas["btcusdt"].base_step, dec("0.000001"));
        assert_eq!(metas["btcusdt"].price_step, dec("0.01"));
        assert_eq!(metas["listausdt"].base_step, dec("1"));
        assert_eq!(metas["listausdt"].price_step, dec("0.000001"));
        assert!(!metas.contains_key("badsymbol"));
    }

    #[test]
    fn normalizes_user_symbols() {
        assert_eq!(normalize_symbol(" LISTA-USDT "), Some("listausdt".into()));
        assert_eq!(normalize_symbol("bTcUsDt"), Some("btcusdt".into()));
        assert_eq!(normalize_symbol("..."), None);
    }

    #[test]
    fn recovery_quote_prefers_the_last_trade_then_the_book() {
        let mut quote = SpotQuote {
            symbol: "dnausdt".into(),
            last: dec("0.04"),
            best_bid: Some(dec("0.0399")),
            best_ask: Some(dec("0.0401")),
            base_step: dec("1"),
            price_step: dec("0.0001"),
        };
        assert_eq!(quote.mark(), Some(dec("0.04")));

        quote.last = Decimal::ZERO;
        assert_eq!(quote.mark(), Some(dec("0.04")), "mid of 0.0399/0.0401");

        quote.best_ask = None;
        assert_eq!(quote.mark(), Some(dec("0.0399")), "bid fallback");

        quote.best_bid = None;
        assert_eq!(quote.mark(), None, "no price means no mark");
    }
}
