# ENRG-HFT

Hybrid **multi-pair** scalping bot — six layers, one binary.

| Layer | Module | Responsibility |
| --- | --- | --- |
| 0.5 Scanner | `src/scanner.rs` | Every `SCAN_INTERVAL_SECS` pulls `GET /market/tickers`, keeps liquid USDT spot pairs (24h quote volume ≥ `SCAN_MIN_QUOTE_VOLUME_USDT`), drops stablecoins and leveraged tokens, ranks the rest by 24h amplitude and reads per-pair `amount-precision` / `price-precision` from `/v1/common/symbols`. |
| 1. Fast hands | `src/exchange/htx.rs` | One HTX WebSocket (`wss://api.htx.com/ws`, fallback `wss://api.huobi.pro/ws`) for **all** scanned pairs: IPv4-only dial, gzip decoding, heartbeat, live `sub`/`unsub` as the watchlist rotates, per-symbol throttled logs, 15s stall watchdog and automatic reconnect. |
| 2. Mathematics | `src/quant_core.rs` | Exact `Decimal` anomaly detection: more than 1% price move inside the last 10 ticks. |
| 2.5 Strategy | `src/strategy.rs` | `MarketBoard`: one isolated book per symbol, anomaly cooldown per symbol, **long-only** `Signal { symbol, side: BUY, price, … }` on a >1% dip (pumps are ignored), and the multi-pair context read by DeepSeek and the executor. |
| 3. Brain | `src/ai_brain.rs` | DeepSeek `deepseek-chat` risk manager with a strict `{"action":"TRADE"\|"PAUSE","symbols":["…"],"reason":"…"}` contract, polled every `AI_INTERVAL_SECS`. |
| 4. Muscles | `src/execution/` | Signed HTX REST client, risk guard, portfolio accounting, paper/live broker and the executor that places, tracks and closes orders **on any pair the AI cleared**. |

The AI is a **gate and a pair selector**, not a signal: a `TRADE` verdict opens
the gate for every symbol in its list for `EXEC_GATE_TTL_SECS`; the quant signal
on any of those symbols decides *when* to buy. The strategy is **long-only**:
a >1% drop inside the anomaly window emits `BUY` (buying the rebound), while a
>1% pump emits nothing — buying the top of a move is not a spot entry. A `SELL`
is never an entry signal: the executor only sells the position it already holds
(take-profit, stop-loss, timeout or kill switch). A stale verdict, a `PAUSE`, a
symbol outside the scanner watchlist or a missing DeepSeek key keeps entries
disabled while the bot keeps streaming data. Pair names are normalised
(`LISTA-USDT` → `listausdt`); a bare base ticker such as `XMR` is accepted only
when `XMRUSDT` is on the watchlist.

## Market scanner

There is no default `btcusdt`. The scanner ranks the whole HTX spot board by
24h range `(high - low) / low` and streams the top `SCAN_TOP_N` pairs:

```text
[SCAN] found 12 active pairs (ranked by 24h amplitude):
[SCAN]  1. LISTAUSDT last=0.1300 chg=30.00% amp=40.00% vol=4200000 spread=15bps
[SCAN]  2. FUNUSDT last=0.0020 chg=17.65% amp=31.25% vol=2500000 spread=10bps
...
```

Filters: non-USDT markets, stablecoin bases (`usdc`, `dai`, `usdd`, …),
leveraged tokens (`*3l`, `*3s`, `*5l`, `*5s`, …), anything below the volume
floor and anything whose quoted spread is wider than `SCAN_MAX_SPREAD_BPS`
(150 bps by default) are discarded. `SCAN_SYMBOLS` can pin extra pairs
(comma-separated), which bypass the volume and spread filters — handy for
debugging one market.

At every rescan the WebSocket subscription list is diffed: new pairs are
`sub`scribed, dropped pairs are `unsub`scribed, and a reconnect always restores
the current list. If the scanner rotates out a pair that still has an open
position, the executor **pins** that symbol so its book keeps streaming until
the position is flat.

## Execution flow

```text
REST tickers ──> Scanner ──> watchlist ──> per-symbol WS subscriptions
                                              │
WS tick ──> MarketBoard book ──> Signal(symbol) ─┐
                                                 v
DeepSeek ──> Verdict(action, symbols) ──> AI gate (TRADE + fresh + symbol in list?)
                                                 v
                                       RiskGuard (size, notional, cooldown, daily loss)
                                                 v
                                       Broker (paper | HTX REST) on that pair
                                                 v
                              Portfolio/PnL ──> TP / SL / timeout ──> exit order
```

- **Paper mode** (default) fills marketable limits at the live touch and rests
  passive ones until the market trades through them. Fees are charged on both
  sides, so paper PnL is honest about spread and commissions.
- **Live mode** signs every request with HMAC-SHA256 (`StringToSign =
  METHOD\nhost\npath\nsorted-params`), sends POST parameters as an
  `application/json` body (only auth fields are signed), prices limit orders
  through the touch with `EXEC_SLIPPAGE_BPS`, polls order state once a second,
  cancels unfilled entries after `EXEC_ORDER_TTL_SECS`, and logs every non-zero
  account balance at boot. The live USDT balance is mirrored into the AI
  context (`equity`/`cash`) every 30 seconds and right after each fill; an entry
  is capped to the spendable balance so a small account is not rejected by the
  venue.
- **Startup recovery** audits the full balance list before the first signal.
  The largest non-USDT coin with a tradable USDT market is restored as the open
  position at the restart mark, pinned into the websocket feed and protected by
  the normal TP/SL/timeout exits. Every other non-USDT balance (stablecoins,
  leveraged tokens, unrelated coins) is sold into USDT with an aggressive
  limit. Balances worth less than HTX's 1 USDT minimum order are logged as
  `ignoring dust` and left untouched: they can never be sold. A balance with no
  USDT market is logged and left untouched because it cannot be priced or sold.
  The original cost basis is not persisted by HTX, so a recovered position is
  anchored to the current mark.
- **Ctrl-C** is a kill switch: the guard latches, working orders are cancelled,
  the position is flattened with one aggressive order and a final report is
  printed.


Spot only: sell entries with a flat book are rejected, so the bot never opens a
short the venue would refuse. One position at a time across all pairs.

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
silently downgrades to paper. Market selection is fully automatic:

```dotenv
SCAN_INTERVAL_SECS=90
SCAN_MIN_QUOTE_VOLUME_USDT=1000000
SCAN_MAX_SPREAD_BPS=150
SCAN_TOP_N=12
# SCAN_SYMBOLS=btcusdt     # optional extra pins
```

Order sizing uses each pair's venue precision from HTX metadata;
`EXEC_BASE_STEP` / `EXEC_PRICE_STEP` are only fallbacks for symbols missing from
`/v1/common/symbols`. Full list of knobs: `.env.example`.

## Run

```bash
cargo build --release
cargo run --release
```

Expected console output (paper mode, abridged):

```text
[BOOT] enrg-hft 0.1.0 starting
[BOOT] scanner: every 90s, min 24h volume 1000000USDT, top 12 pairs
[BOOT] execution=paper order_notional=25USDT paper_cash=1000USDT
[SCAN] found 12 active pairs (ranked by 24h amplitude):
[SCAN]  1. LISTAUSDT last=0.1300 chg=30.00% amp=40.00% vol=4200000
...
[WS] connected to wss://api.htx.com/ws over IPv4 (HTTP 101 Switching Protocols)
[WS] subscribed: market.listausdt.depth.step0
[WS] subscribed: market.listausdt.trade.detail
...
[WS] streaming 12 symbols over one socket
[EXEC] mode=paper pairs=12 order_notional=25USDT tp=0.8% sl=0.5% max_hold=120s gate_ttl=120s
[TICK] LISTAUSDT price=0.1302 latency=41ms
[DEPTH] FUNUSDT bid=0.001997 ask=0.002003
[AI] 2026-11-14 16:27:00 UTC asking DeepSeek about 12 pairs:
watchlist:
1. LISTAUSDT last=0.1302 bid=0.1301 ask=0.1302 spread=7.7bps chg24=30% amp24=40% vol24=4200000 anomaly=yes
...
account: equity=1000 cash=1000 position_symbol=- position_qty=0 ...
[AI] 2026-11-14 16:27:02 UTC decision: TRADE [LISTAUSDT, FUNUSDT, TRUMPUSDT] — momentum across the watchlist
[EXEC] AI gate TRADE 3 pairs: LISTAUSDT, FUNUSDT, TRUMPUSDT — momentum across the watchlist
[SIGNAL] LISTAUSDT BUY 0.1304 (1.21%) — >1% dip across 10 ticks (buying the rebound)
[EXEC] placing BUY 191.7 LISTAUSDT @ 0.1305 (signal: >1% dip across 10 ticks (buying the rebound))
[EXEC] order paper-1 accepted: filled
[EXEC] entry fill 191.7 LISTAUSDT @ 0.1305 (fee 0.0500)
[EXEC] closing position: SELL 191.7 LISTAUSDT @ 0.1316 — take-profit (pnl 0.84%)
[EXEC] exit fill 191.7 LISTAUSDT @ 0.1316 (fee 0.0504)
[EXEC] trade #1 closed: net 0.1105 USDT (1 of 1 winners)
```

## Safety notes

- Default mode is `paper`; live trading is an explicit opt-in.
- The AI can only open the pair it named, and only if that pair is on the
  scanner watchlist; a hallucinated ticker is rejected and logged.
- Order amounts are rounded **down** to the pair's base step, never above the
  configured notional.
- Prices are rounded **outward** to the pair's price step, so an "aggressive"
  limit actually crosses the touch.
- A missing or failing DeepSeek key leaves the gate closed: no entries, but the
  market feed, risk guard and positions remain live.
- An exit rejected with HTX `order-value-min-error` is logged once and the
  residual is dropped from local state, so the bot never spams the venue with an
  order that can never be filled.
- The executor never blocks the socket: it reads only the books of its own
  working order / open position from the shared board and trades from its own
  task.

## Tests

```bash
cargo test
```

119 unit tests, no network required: Decimal anomaly windows, scanner filtering
and ranking, per-symbol books and watchlist pinning, DeepSeek JSON
normalisation with multi-pair selection, HTX frame parsing/gzip/channel routing,
request signing against an independent golden vector, order/balance parsers,
portfolio PnL, every risk rejection, the paper broker's fill semantics and
symbol isolation, sizing/rounding helpers, startup balance recovery (primary
selection, dust filtering, unquoted assets, stop-loss arming) and full paper
round-trips through the multi-pair executor. The exit path is also covered for
the venue-minimum error that drops an unsellable residual instead of retrying.
