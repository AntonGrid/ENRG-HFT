# ENRG-HFT

Hybrid scalping bot — four layers, one binary.

| Layer | Module | Responsibility |
| --- | --- | --- |
| 1. Fast hands | `src/exchange/htx.rs` | HTX WebSocket (`wss://api.htx.com/ws`, fallback `wss://api.huobi.pro/ws`): IPv4-only dial, gzip decoding, heartbeat, depth/trade subscriptions, nanosecond tick stamps, 15s stall watchdog and automatic reconnect. Symbol comes from `HTX_SYMBOL`. |
| 2. Mathematics | `src/quant_core.rs` | Exact `Decimal` anomaly detection: more than 1% price move inside the last 10 ticks. |
| 2.5 Strategy | `src/strategy.rs` | Rolling window, top of book, anomaly cooldown, directional `Signal`, and the shared `MarketSnapshot` read by the AI and the executor. |
| 3. Brain | `src/ai_brain.rs` | DeepSeek `deepseek-chat` risk manager, strict `{"action":"TRADE"\|"PAUSE","reason":"..."}` contract, polled every `AI_INTERVAL_SECS`. |
| 4. Muscles | `src/execution/` | Signed HTX REST client, risk guard, portfolio accounting, paper/live broker and the executor that places, tracks and closes orders. |

The AI is a **gate**, not a signal: a `TRADE` verdict opens the gate for
`EXEC_GATE_TTL_SECS`; the quant signal decides *when* and in which direction
(momentum across the anomaly window). A stale verdict, a `PAUSE`, or a missing
DeepSeek key keeps entries disabled while the bot keeps streaming data.

## Execution flow

```text
WS tick ──> Strategy ──> Signal ─────────────┐
                                             v
DeepSeek ──> Verdict ────────────────> AI gate (TRADE + fresh?)
                                             v
                                   RiskGuard (size, notional, cooldown, daily loss)
                                             v
                                   Broker (paper | HTX REST)
                                             v
                            Portfolio/PnL ──> TP / SL / timeout ──> exit order
```

- **Paper mode** (default) fills marketable limits at the live touch and rests
  passive ones until the market trades through them. Fees are charged on both
  sides, so paper PnL is honest about spread and commissions.
- **Live mode** signs every request with HMAC-SHA256 (`StringToSign =
  METHOD\nhost\npath\nsorted-params`), sends limit orders priced through the
  touch with `EXEC_SLIPPAGE_BPS`, polls order state once a second, cancels
  unfilled entries after `EXEC_ORDER_TTL_SECS`, and logs account balances at
  boot.
- **Ctrl-C** is a kill switch: the guard latches, working orders are cancelled,
  the position is flattened with one aggressive order and a final report is
  printed.

Spot only: sell entries with a flat book are rejected, so the bot never opens a
short the venue would refuse.

## Risk model

| Knob | Default | Meaning |
| --- | --- | --- |
| `EXEC_ORDER_NOTIONAL_USDT` | 25 | Target notional per entry |
| `EXEC_MIN_NOTIONAL_USDT` | 5 | Reject dust orders |
| `EXEC_MAX_ORDER_NOTIONAL_USDT` | 50 | Hard per-order cap |
| `EXEC_MAX_POSITION_NOTIONAL_USDT` | 50 | Hard exposure cap (one position at a time) |
| `EXEC_MAX_DAILY_LOSS_USDT` | 20 | Loss budget; latches entries until the next UTC day |
| `EXEC_TAKE_PROFIT_PCT` | 0.8 | Close at +0.8% |
| `EXEC_STOP_LOSS_PCT` | 0.5 | Close at -0.5% |
| `EXEC_MAX_HOLD_SECS` | 120 | Time-based exit |
| `EXEC_COOLDOWN_SECS` | 10 | Pause between accepted entries |

Exits bypass the daily-loss halt and the cooldown, so an open position can
always be closed.

## Configuration

Secrets live in `.env` (ignored by git); copy `.env.example` and fill it in.
The important switch is:

```dotenv
EXECUTION_MODE=paper   # off | paper | live
```

`live` refuses to start without `HTX_API_KEY` / `HTX_API_SECRET` — it never
silently downgrades to paper. `HTX_SYMBOL` (default `btcusdt`) drives market
data, order symbols, base-step filtering and the AI context. Full list of
knobs: `.env.example`.

## Run

```bash
cargo build --release
cargo run --release
```

Expected console output (paper mode):

```text
[BOOT] enrg-hft 0.1.0 starting
[BOOT] symbol=btcusdt execution=paper order_notional=25USDT paper_cash=1000USDT
[WS] connected to wss://api.htx.com/ws over IPv4 (HTTP 101 Switching Protocols)
[WS] market: btcusdt
[WS] subscribed: market.btcusdt.depth.step0
[WS] subscribed: market.btcusdt.trade.detail
[EXEC] mode=paper symbol=btcusdt order_notional=25USDT tp=0.8% sl=0.5% max_hold=120s gate_ttl=120s
[AI] 2026-11-14 16:27:00 UTC asking DeepSeek: symbol=btcusdt last=65447.0 ...
[AI] 2026-11-14 16:27:02 UTC decision: TRADE — spread is tight, volatility expanding
[EXEC] AI gate TRADE — spread is tight, volatility expanding
[SIGNAL] BUY 65610.5 (1.21%) — >1% range in the last 10 ticks
[EXEC] placing BUY 0.00038 btcusdt @ 65643.32 (signal: >1% range in the last 10 ticks)
[EXEC] order paper-1 accepted: filled
[EXEC] entry fill 0.00038 btcusdt @ 65610.5 (fee 0.004986)
[EXEC] closing position: SELL 0.00038 @ 66130.0 — take-profit (pnl 0.792%)
[EXEC] exit fill 0.00038 btcusdt @ 66130.0 (fee 0.005026)
[EXEC] trade #1 closed: net 0.1875 USDT (1 of 1 winners)
```

## Safety notes

- Default mode is `paper`; live trading is an explicit opt-in.
- Order amounts are rounded **down** to `EXEC_BASE_STEP`, never above the
  configured notional.
- Prices are rounded **outward** to `EXEC_PRICE_STEP`, so an "aggressive" limit
  actually crosses the touch.
- A missing or failing DeepSeek key leaves the gate closed: no entries, but the
  market feed, risk guard and positions remain live.
- The executor never blocks the socket: it reads a `watch` snapshot and trades
  from its own task.

## Tests

```bash
cargo test
```

80 unit tests, no network required: Decimal anomaly windows, DeepSeek JSON
normalisation, HTX frame parsing/gzip, request signing against an independent
golden vector, order/balance parsers, portfolio PnL, every risk rejection, the
paper broker's fill semantics, sizing/rounding helpers and a full paper
round-trip through the executor.
