# ENRG-HFT

Hybrid scalping bot — three layers, one binary.

| Layer | Module | Responsibility |
| --- | --- | --- |
| 1. Fast hands | `src/exchange/htx.rs` | HTX WebSocket (`wss://api.htx.com/ws`, with `wss://api.huobi.pro/ws` as fallback): IPv4-only dial, gzip decoding, heartbeat, depth/trade subscriptions, nanosecond tick stamps, 15s stall watchdog and automatic reconnect |
| 2. Mathematics | `src/quant_core.rs` | Exact `Decimal` anomaly detection: more than 1% price move inside the last 10 ticks |
| 3. Brain | `src/ai_brain.rs` | DeepSeek `deepseek-chat` risk manager, strict `{"action":"TRADE"\|"PAUSE","reason":"..."}` contract |

`src/main.rs` wires the layers together: the socket runs in its own task, the
quant core consumes a bounded channel, and the brain is polled every 60 seconds.
No orders are placed in this sprint.

## Configuration

Secrets live in `.env` (ignored by git):

```dotenv
DEEPSEEK_API_KEY=...
HTX_API_KEY=...
HTX_API_SECRET=...
# Optional: pin a single market-data endpoint instead of the failover list.
# HTX_WS_URL=wss://api.htx.com/ws
```

The public CloudFront edge `api.huobi.pro` answers the WebSocket handshake
from this VPS but sends no market frames, so `api.htx.com` (the same HTX
spot venue) is dialled first. A silent endpoint is abandoned after 15 seconds
and the next URL in the list is tried.

## Run

```bash
cargo build --release
cargo run --release
```

Expected console output:

```text
[WS] connected to wss://api.htx.com/ws over IPv4 (HTTP 101 Switching Protocols)
[WS] subscribed: market.btcusdt.depth.step0
[WS] subscribed: market.btcusdt.trade.detail
[WS] subscription confirmed: market.btcusdt.depth.step0
[DEPTH] bid=65440.87 ask=65440.88 | Time: 1794673621409690035
[TICK] Price: 65447.0 | Time: 1794673621409742769 | Latency: 1697ms
[AI] 2026-11-14 16:27:00 UTC decision: PAUSE — Price stable, no dislocation
```

## Tests

```bash
cargo test
```

The unit tests cover Decimal anomaly windows, DeepSeek JSON normalisation and
HTX frame parsing/gzip decompression; none of them need network access.
