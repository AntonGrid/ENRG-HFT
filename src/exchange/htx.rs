//! Layer 1 — fast hands.
//!
//! A read-only HTX market-data client. It stays mercilessly dumb: decompress,
//! answer heartbeats, parse, stamp with a nanosecond clock, hand the tick to the
//! rest of the bot. No trading decisions live here.

use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::Read;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::read::{GzDecoder, ZlibDecoder};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::net::{lookup_host, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, timeout};
use tokio_native_tls::TlsStream;
use tokio_tungstenite::client_async_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// HTX spot market-data endpoints. They are the same venue behind different
/// edges; `api.htx.com` is first because the `api.huobi.pro` CloudFront edge
/// completes handshakes from this VPS but never delivers frames.
const DEFAULT_WS_URLS: [&str; 2] = ["wss://api.htx.com/ws", "wss://api.huobi.pro/ws"];
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// HTX pings roughly once a second. A silent endpoint is treated as dead and
/// the client fails over to the next URL.
const READ_TIMEOUT: Duration = Duration::from_secs(15);

/// HTX channel templates. The symbols come from the scanner; there is no
/// default pair anywhere in the market-data path.
const DEPTH_TOPIC: &str = "depth.step0";
const TRADE_TOPIC: &str = "trade.detail";

/// Capacity of the WS -> strategy channel. If the consumer falls behind, market
/// data is dropped rather than the socket being blocked.
pub const EVENT_BUFFER: usize = 8192;

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const LOG_INTERVAL: Duration = Duration::from_secs(1);

/// One symbol's two channels.
fn channel_names(symbol: &str) -> [String; 2] {
    [
        format!("market.{symbol}.{DEPTH_TOPIC}"),
        format!("market.{symbol}.{TRADE_TOPIC}"),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelKind {
    Trade,
    Depth,
}

/// Parses `market.<symbol>.depth.step0` / `market.<symbol>.trade.detail`.
fn parse_channel(channel: &str) -> Option<(&str, ChannelKind)> {
    let rest = channel.strip_prefix("market.")?;
    let (symbol, topic) = rest.split_once('.')?;
    if symbol.is_empty() {
        return None;
    }
    if topic.starts_with("depth") {
        Some((symbol, ChannelKind::Depth))
    } else if topic.starts_with("trade") {
        Some((symbol, ChannelKind::Trade))
    } else {
        None
    }
}

/// Per-symbol log throttling: one line per second per pair instead of
/// thousands of lines a second across a dozen markets.
#[derive(Debug, Default)]
struct LogThrottle {
    last: HashMap<String, Instant>,
}

impl LogThrottle {
    fn due(&mut self, key: &str) -> bool {
        if self.last.len() > 512 {
            self.last.clear();
        }
        let now = Instant::now();
        match self.last.get(key) {
            Some(last) if now.duration_since(*last) < LOG_INTERVAL => false,
            _ => {
                self.last.insert(key.to_string(), now);
                true
            }
        }
    }
}

type BoxError = Box<dyn Error + Send + Sync>;

type WsStream = WebSocketStream<TlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;

/// Events handed to the rest of the bot. Every event carries the symbol that
/// produced it, so one socket can feed a dozen isolated books.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum MarketEvent {
    Trade {
        symbol: String,
        price: Decimal,
        /// Base-asset size of the print, used by the volume-spike filter.
        amount: Decimal,
        exchange_ts_ms: i64,
        received_ns: u128,
    },
    Depth {
        symbol: String,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
        exchange_ts_ms: i64,
        received_ns: u128,
    },
}

impl MarketEvent {
    pub fn symbol(&self) -> &str {
        match self {
            Self::Trade { symbol, .. } | Self::Depth { symbol, .. } => symbol,
        }
    }
}

/// A parsed `wss://host[:port]/path` endpoint.
#[derive(Debug, Clone)]
struct Endpoint {
    url: String,
    host: String,
    port: u16,
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self, BoxError> {
        let url = url.trim();
        let rest = url.strip_prefix("wss://").ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("only wss:// endpoints are supported, got {url:?}"),
            )
        })?;

        let (authority, path) = match rest.split_once('/') {
            Some((authority, path)) => (authority, format!("/{path}")),
            None => (rest, "/ws".to_string()),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), port.parse::<u16>()?),
            None => (authority.to_string(), 443),
        };

        if host.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("endpoint {url:?} has an empty host"),
            )
            .into());
        }

        Ok(Self {
            url: format!("wss://{authority}{path}"),
            host,
            port,
        })
    }
}

/// `HTX_WS_URL` overrides the failover list, which is handy for paper trading
/// against another venue or a local recorder.
fn configured_endpoints() -> Vec<Endpoint> {
    if let Ok(url) = std::env::var("HTX_WS_URL") {
        let url = url.trim();
        if !url.is_empty() {
            match Endpoint::parse(url) {
                Ok(endpoint) => return vec![endpoint],
                Err(err) => eprintln!("[WS] ignoring invalid HTX_WS_URL={url:?}: {err}"),
            }
        }
    }

    DEFAULT_WS_URLS
        .iter()
        .filter_map(|url| match Endpoint::parse(url) {
            Ok(endpoint) => Some(endpoint),
            Err(err) => {
                eprintln!("[WS] invalid built-in endpoint {url}: {err}");
                None
            }
        })
        .collect()
}

/// Connects to HTX and keeps the socket alive forever. The symbol watchlist is
/// driven by the scanner: subscriptions are added and removed live, and a
/// reconnect re-subscribes to whatever the list is at that moment.
pub async fn run_market_stream(
    tx: mpsc::Sender<MarketEvent>,
    mut symbols: watch::Receiver<Vec<String>>,
) {
    let endpoints = configured_endpoints();
    if endpoints.is_empty() {
        eprintln!("[WS] no usable endpoints; market data is disabled");
        return;
    }

    loop {
        // Nothing to subscribe to yet: wait for the scanner without opening a
        // socket that would only exchange heartbeats.
        let mut waiting_logged = false;
        while symbols.borrow().is_empty() {
            if !waiting_logged {
                println!("[WS] waiting for the scanner to select pairs…");
                waiting_logged = true;
            }
            if symbols.changed().await.is_err() {
                return;
            }
        }

        let mut index = 0;
        loop {
            let endpoint = &endpoints[index % endpoints.len()];
            match connect_and_stream(endpoint, &tx, &mut symbols).await {
                Ok(()) => eprintln!("[WS] {} stream ended", endpoint.url),
                Err(err) => eprintln!("[WS] {}: {err}", endpoint.url),
            }

            if symbols.has_changed().is_err() {
                return;
            }
            if symbols.borrow().is_empty() {
                break;
            }

            if endpoints.len() > 1 {
                index = (index + 1) % endpoints.len();
            }
            eprintln!("[WS] reconnecting in {}s", RECONNECT_DELAY.as_secs());
            sleep(RECONNECT_DELAY).await;
        }
    }
}

/// Opens the socket over IPv4 only.
///
/// HTX edges advertise both IPv6 and IPv4. On this VPS the IPv6 path completes
/// the TLS handshake but never delivers market data, so the address family is
/// pinned to IPv4 before the WebSocket upgrade. TLS still uses the endpoint
/// host name for SNI.
async fn open_socket(
    endpoint: &Endpoint,
) -> Result<WebSocketStream<TlsStream<TcpStream>>, BoxError> {
    let addresses: Vec<SocketAddr> = lookup_host((endpoint.host.as_str(), endpoint.port))
        .await?
        .filter(|address| address.is_ipv4())
        .collect();

    let mut last_error = None;
    let mut tcp = None;
    for address in addresses {
        match timeout(CONNECT_TIMEOUT, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                tcp = Some(stream);
                break;
            }
            Ok(Err(err)) => last_error = Some(format!("{address}: {err}")),
            Err(_) => last_error = Some(format!("{address}: connect timed out")),
        }
    }

    let tcp = tcp.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            format!(
                "no reachable IPv4 address for {}: {}",
                endpoint.host,
                last_error.unwrap_or_else(|| "DNS returned no IPv4 address".to_string())
            ),
        )
    })?;
    tcp.set_nodelay(true)?;

    let tls_connector = native_tls::TlsConnector::builder().build()?;
    let tls = tokio_native_tls::TlsConnector::from(tls_connector)
        .connect(endpoint.host.as_str(), tcp)
        .await?;

    let request = endpoint.url.as_str().into_client_request()?;
    let (socket, response) = client_async_with_config(request, tls, None).await?;
    println!(
        "[WS] connected to {} over IPv4 (HTTP {})",
        endpoint.url,
        response.status()
    );

    Ok(socket)
}

async fn connect_and_stream(
    endpoint: &Endpoint,
    tx: &mpsc::Sender<MarketEvent>,
    symbols: &mut watch::Receiver<Vec<String>>,
) -> Result<(), BoxError> {
    let socket = open_socket(endpoint).await?;
    let (mut sink, mut source) = socket.split();

    let mut subscribed: BTreeSet<String> = BTreeSet::new();
    let initial: Vec<String> = symbols.borrow().iter().cloned().collect();
    apply_subscriptions(&mut sink, &initial, true).await?;
    subscribed.extend(initial);
    println!(
        "[WS] streaming {} symbols over one socket",
        subscribed.len()
    );

    let mut throttle = LogThrottle::default();

    loop {
        tokio::select! {
            biased;
            changed = symbols.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                let desired: BTreeSet<String> = symbols.borrow().iter().cloned().collect();
                let added: Vec<String> = desired.difference(&subscribed).cloned().collect();
                let removed: Vec<String> = subscribed.difference(&desired).cloned().collect();
                if added.is_empty() && removed.is_empty() {
                    continue;
                }

                apply_subscriptions(&mut sink, &removed, false).await?;
                for symbol in &removed {
                    subscribed.remove(symbol);
                }
                apply_subscriptions(&mut sink, &added, true).await?;
                subscribed.extend(added);
                println!("[WS] watchlist updated: {} symbols", subscribed.len());
            }
            message = timeout(READ_TIMEOUT, source.next()) => {
                let message = match message {
                    Ok(Some(frame)) => frame?,
                    Ok(None) => break,
                    Err(_) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "no frames for {}s (endpoint is silent)",
                                READ_TIMEOUT.as_secs()
                            ),
                        )
                        .into())
                    }
                };

                let payload = match message {
                    Message::Text(text) => text,
                    Message::Binary(bytes) => match decompress(&bytes) {
                        Ok(text) => text,
                        Err(err) => {
                            eprintln!("[WS] gzip decode failed: {err}");
                            continue;
                        }
                    },
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Close(frame) => {
                        println!("[WS] closed by server: {frame:?}");
                        break;
                    }
                    Message::Pong(_) | Message::Frame(_) => continue,
                };

                let value: Value = match serde_json::from_str(&payload) {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!("[WS] bad JSON frame: {err}");
                        continue;
                    }
                };

                // HTX heartbeat: {"ping": 123456}. Without an immediate pong
                // the server drops the connection.
                if let Some(ping) = value.get("ping") {
                    sink.send(Message::Text(json!({ "pong": ping }).to_string()))
                        .await?;
                    continue;
                }

                route_payload(&value, tx, &mut throttle);
            }
        }
    }

    Ok(())
}

/// Subscribes or unsubscribes both channels of every symbol.
async fn apply_subscriptions(
    sink: &mut WsSink,
    symbols: &[String],
    subscribe: bool,
) -> Result<(), BoxError> {
    for symbol in symbols {
        for channel in channel_names(symbol) {
            let id = format!("{}-{channel}", if subscribe { "sub" } else { "unsub" });
            let request = if subscribe {
                json!({ "sub": channel, "id": id })
            } else {
                json!({ "unsub": channel, "id": id })
            };
            sink.send(Message::Text(request.to_string())).await?;
            println!(
                "[WS] {} {channel}",
                if subscribe {
                    "subscribed:"
                } else {
                    "unsubscribed:"
                }
            );
        }
    }
    Ok(())
}

fn route_payload(value: &Value, tx: &mpsc::Sender<MarketEvent>, throttle: &mut LogThrottle) {
    if let Some(channel) = value.get("ch").and_then(Value::as_str) {
        if let Some((symbol, kind)) = parse_channel(channel) {
            match kind {
                ChannelKind::Trade => handle_trades(value, symbol, tx, throttle),
                ChannelKind::Depth => handle_depth(value, symbol, tx, throttle),
            }
            return;
        }
    }

    if value.get("status").and_then(Value::as_str) == Some("ok") {
        if let Some(subbed) = value.get("subbed").and_then(Value::as_str) {
            println!("[WS] subscription confirmed: {subbed}");
        } else if let Some(unsubbed) = value.get("unsubbed").and_then(Value::as_str) {
            println!("[WS] unsubscription confirmed: {unsubbed}");
        }
    }
}

/// Forwards every print and logs one line per second per symbol.
fn handle_trades(
    value: &Value,
    symbol: &str,
    tx: &mpsc::Sender<MarketEvent>,
    throttle: &mut LogThrottle,
) {
    let Some(tick) = value.get("tick") else {
        return;
    };
    let message_ts = tick
        .get("ts")
        .and_then(Value::as_i64)
        .or_else(|| value.get("ts").and_then(Value::as_i64))
        .unwrap_or(0);
    let Some(trades) = tick.get("data").and_then(Value::as_array) else {
        return;
    };

    for trade in trades {
        let Some(price) = trade.get("price").and_then(decimal_from_value) else {
            continue;
        };
        let amount = trade
            .get("amount")
            .and_then(decimal_from_value)
            .unwrap_or(Decimal::ZERO);
        let exchange_ts_ms = trade
            .get("ts")
            .and_then(Value::as_i64)
            .unwrap_or(message_ts);
        let received_ns = unix_nanos();

        if throttle.due(&format!("trade:{symbol}")) {
            let latency_ms = if exchange_ts_ms > 0 {
                (received_ns / 1_000_000) as i64 - exchange_ts_ms
            } else {
                0
            };
            println!(
                "[TICK] {} price={} latency={}ms",
                symbol.to_uppercase(),
                price,
                latency_ms
            );
        }

        // Historical trade-detail frames may contain several prints; forward
        // every one of them, and drop rather than block if the consumer stalls.
        let _ = tx.try_send(MarketEvent::Trade {
            symbol: symbol.to_string(),
            price,
            amount,
            exchange_ts_ms,
            received_ns,
        });
    }
}

/// Tracks the top of book. Depth arrives far too often for line-per-frame
/// logging, so the console output is throttled to once a second per symbol;
/// every frame is still forwarded to the bot.
fn handle_depth(
    value: &Value,
    symbol: &str,
    tx: &mpsc::Sender<MarketEvent>,
    throttle: &mut LogThrottle,
) {
    let Some(tick) = value.get("tick") else {
        return;
    };
    let exchange_ts_ms = tick
        .get("ts")
        .and_then(Value::as_i64)
        .or_else(|| value.get("ts").and_then(Value::as_i64))
        .unwrap_or(0);
    let best_bid = first_level_price(tick.get("bids"));
    let best_ask = first_level_price(tick.get("asks"));
    let received_ns = unix_nanos();

    if throttle.due(&format!("depth:{symbol}")) {
        println!(
            "[DEPTH] {} bid={} ask={}",
            symbol.to_uppercase(),
            price_text(best_bid),
            price_text(best_ask)
        );
    }

    let _ = tx.try_send(MarketEvent::Depth {
        symbol: symbol.to_string(),
        best_bid,
        best_ask,
        exchange_ts_ms,
        received_ns,
    });
}

fn first_level_price(levels: Option<&Value>) -> Option<Decimal> {
    levels?
        .as_array()?
        .first()?
        .as_array()?
        .first()
        .and_then(decimal_from_value)
}

fn price_text(price: Option<Decimal>) -> String {
    price
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn decimal_from_value(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str(text).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

/// HTX sends gzip-compressed binary frames. `GzDecoder` handles the documented
/// format; zlib and plain UTF-8 are accepted as fallbacks so a venue-side change
/// does not take the feed down.
fn decompress(bytes: &[u8]) -> Result<String, BoxError> {
    if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(bytes);
        let mut text = String::new();
        decoder.read_to_string(&mut text)?;
        return Ok(text);
    }

    if let Ok(text) = std::str::from_utf8(bytes) {
        if text.trim_start().starts_with('{') {
            return Ok(text.to_owned());
        }
    }

    let mut decoder = ZlibDecoder::new(bytes);
    let mut text = String::new();
    decoder.read_to_string(&mut text)?;
    Ok(text)
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    #[test]
    fn decompresses_gzip_payload() {
        let source = r#"{"ping":123456}"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(source.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        assert_eq!(decompress(&compressed).unwrap(), source);
    }

    #[test]
    fn passes_through_plain_json() {
        let source = r#"{"pong":123456}"#;
        assert_eq!(decompress(source.as_bytes()).unwrap(), source);
    }

    #[test]
    fn parses_htx_trade_tick_as_decimal() {
        let payload = serde_json::json!({
            "ch": "market.btcusdt.trade.detail",
            "ts": 1_763_112_345_678i64,
            "tick": {
                "id": 7,
                "ts": 1_763_112_345_678i64,
                "data": [{
                    "id": 7,
                    "ts": 1_763_112_345_600i64,
                    "price": 96553.12,
                    "amount": 0.001,
                    "direction": "buy"
                }]
            }
        });

        let (tx, mut rx) = mpsc::channel(8);
        let mut throttle = LogThrottle::default();
        route_payload(&payload, &tx, &mut throttle);

        match rx.try_recv().expect("trade event must be forwarded") {
            MarketEvent::Trade {
                symbol,
                price,
                amount,
                exchange_ts_ms,
                ..
            } => {
                assert_eq!(symbol, "btcusdt");
                assert_eq!(price, Decimal::from_str("96553.12").unwrap());
                assert_eq!(amount, Decimal::from_str("0.001").unwrap());
                assert_eq!(exchange_ts_ms, 1_763_112_345_600);
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[test]
    fn parses_depth_top_of_book() {
        let payload = serde_json::json!({
            "ch": "market.listausdt.depth.step0",
            "tick": {
                "ts": 1_763_112_345_678i64,
                "bids": [["96553.12", "0.5"]],
                "asks": [[96553.99, 0.25]]
            }
        });

        let (tx, mut rx) = mpsc::channel(8);
        let mut throttle = LogThrottle::default();
        route_payload(&payload, &tx, &mut throttle);

        match rx.try_recv().expect("depth event must be forwarded") {
            MarketEvent::Depth {
                symbol,
                best_bid,
                best_ask,
                ..
            } => {
                assert_eq!(symbol, "listausdt");
                assert_eq!(best_bid, Some(Decimal::from_str("96553.12").unwrap()));
                assert_eq!(best_ask, Some(Decimal::from_str("96553.99").unwrap()));
            }
            other => panic!("expected depth, got {other:?}"),
        }
    }

    #[test]
    fn channel_names_are_built_per_scanned_symbol() {
        let channels = channel_names("listausdt");
        assert_eq!(channels[0], "market.listausdt.depth.step0");
        assert_eq!(channels[1], "market.listausdt.trade.detail");

        assert_eq!(
            parse_channel("market.listausdt.depth.step0"),
            Some(("listausdt", ChannelKind::Depth))
        );
        assert_eq!(
            parse_channel("market.listausdt.trade.detail"),
            Some(("listausdt", ChannelKind::Trade))
        );
        assert_eq!(parse_channel("market.listausdt.kline.1min"), None);
        assert_eq!(parse_channel("notifications"), None);
    }

    #[test]
    fn log_throttle_limits_per_symbol_output() {
        let mut throttle = LogThrottle::default();
        assert!(throttle.due("trade:btcusdt"));
        assert!(!throttle.due("trade:btcusdt"));
        assert!(throttle.due("trade:listausdt"));
    }

    #[test]
    fn parses_wss_endpoint() {
        let endpoint = Endpoint::parse("wss://api.htx.com/ws").expect("endpoint must parse");
        assert_eq!(endpoint.host, "api.htx.com");
        assert_eq!(endpoint.port, 443);
        assert_eq!(endpoint.url, "wss://api.htx.com/ws");
    }

    #[test]
    fn defaults_path_and_keeps_explicit_port() {
        let endpoint = Endpoint::parse("wss://example.com:8443").expect("endpoint must parse");
        assert_eq!(endpoint.host, "example.com");
        assert_eq!(endpoint.port, 8443);
        assert_eq!(endpoint.url, "wss://example.com:8443/ws");
    }

    #[test]
    fn rejects_non_wss_endpoint() {
        assert!(Endpoint::parse("https://api.htx.com/ws").is_err());
    }
}
