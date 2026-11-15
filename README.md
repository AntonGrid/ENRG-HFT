# ENRG-HFT

Hybrid **multi-pair** scalping bot — six layers, one binary.

| Layer | Module | Responsibility |
| --- | --- | --- |
| 0.5 Scanner | `src/scanner.rs` | Every `SCAN_INTERVAL_SECS` pulls `GET /market/tickers`, keeps liquid USDT spot pairs (24h quote volume ≥ `SCAN_MIN_QUOTE_VOLUME_USDT`), drops stablecoins and leveraged tokens, ranks the rest by 24h amplitude and reads per-pair `amount-precision` / `price-precision` from `/v1/common/symbols`. |
| 1. Fast hands | `src/exchange/htx.rs` | One HTX WebSocket (`wss://api.htx.com/ws`, fallback `wss://api.huobi.pro/ws`) for **all** scanned pairs: IPv4-only dial, gzip decoding, heartbeat, live `sub`/`unsub` as the watchlist rotates, per-symbol throttled logs, 15s stall watchdog and automatic reconnect. |
| 2. Mathematics | `src/quant_core.rs` | Exact `Decimal` anomaly detection: more than 1% price move inside the last 10 ticks. |
| 2.5 Strategy | `src/strategy.rs` | `MarketBoard`: one isolated book per symbol, anomaly cooldown per symbol, **long-only** `Signal { symbol, side: BUY, price, kind, volume_spike, … }` on a >1% dip or a >1% momentum breakout, a rolling 1-minute quote-volume versus 24h-average filter, and the multi-pair context read by DeepSeek and the executor. |
| 3. Brain | `src/ai_brain.rs` | DeepSeek `deepseek-chat` risk manager with a strict `{"action":"TRADE"\|"PAUSE","symbols":["…"],"reason":"…"}` contract, polled every `AI_INTERVAL_SECS`. |
| 4. Muscles | `src/execution/` | Signed HTX REST client, risk guard, portfolio accounting, paper/live broker and the executor that places, tracks and closes orders **on any pair the AI cleared**. |

The AI is a **gate and a pair selector**, not a signal: a `TRADE` verdict opens
the gate for every symbol in its list for `EXEC_GATE_TTL_SECS`; the quant signal
on any of those symbols decides *when* to buy. The strategy is **long-only** and
trades two dislocations of the same anomaly window:

- **Dip** — a >1% drop emits `BUY` (buy the rebound);
- **Momentum** — a >1% rise emits `BUY` (ride the continuation). The trailing
  exit is what makes this asymmetric: a trend keeps running until it turns,
  and only a 0.7% hard stop caps the downside.

A `SELL` is never an entry signal: the executor only sells the position it
already holds (trailing take-profit, hard stop-loss or kill switch). An entry is
only tradable when its last minute carries at least **2×** the normal one-minute
volume (the larger of the previous minute and the 24h average), so a quiet drift
is ignored. Both kinds share one cooldown, so a pump and the dip after it cannot
fire back to back.
A stale verdict, a `PAUSE`, a symbol outside the scanner watchlist or a missing
DeepSeek key keeps entries disabled while the bot keeps streaming data. Pair
names are normalised (`LISTA-USDT` → `listausdt`); a bare base ticker such as
`XMR` is accepted only when `XMRUSDT` is on the watchlist.

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
(40 bps by default) are discarded. `SCAN_SYMBOLS` can pin extra pairs
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
                              Portfolio/PnL ──> trailing TP / hard SL ──> exit order
```

- **Paper mode** (default) fills market orders at the live touch, rejects a
  post-only limit that would cross, and rests passive limits until the market
  trades through them. Fees are charged on both sides, so paper PnL is honest
  about spread and commissions.
- **Live mode** signs every request with HMAC-SHA256 (`StringToSign =
  METHOD\nhost\npath\nsorted-params`), sends POST parameters as an
  `application/json` body (only auth fields are signed), polls order state once
  a second, and logs every non-zero account balance at boot.
  Entries use `*-limit-maker` (post-only): a buy rests on the best bid, so it
  never pays taker fees or crosses the spread. Both exits use `*-market` orders,
  because getting out matters more than the fee: the hard stop must fire at any
  cost, and a trailing stop that cannot fill is just a delayed stop-loss. An
  accepted maker entry immediately appears in the local `Portfolio`
  (`position_qty` / `avg_entry`, with paper cash reserved), so the AI context
  and risk guard never see a flat book while the order is still resting; a
  cancel or reject rolls the overlay back exactly. Unfilled maker entries are
  cancelled after `EXEC_ORDER_TTL_SECS`.
  The live USDT balance is mirrored into the AI context (`equity`/`cash`) every
  30 seconds and right after each fill; an entry is capped to the spendable
  balance so a small account is not rejected by the venue.
- **Startup recovery** audits the full balance list before the first signal.
  The largest non-USDT coin with a tradable USDT market is restored as the open
  position at the restart mark, pinned into the websocket feed and protected by
  the same trailing-stop / hard-stop exits. Every other non-USDT balance (stablecoins,
  leveraged tokens, unrelated coins) is sold into USDT with a **market** order.
  Balances worth less than HTX's 1 USDT minimum order are logged as
  `ignoring dust` and left untouched: they can never be sold. A balance with no
  USDT market is logged and left untouched because it cannot be priced or sold.
  The original cost basis is not persisted by HTX, so a recovered position is
  anchored to the current mark.
- **Ctrl-C** is a kill switch: the guard latches, working orders are cancelled,
  the position is flattened with one **market** order and a final report is
  printed.

## Exit scheme

There is no fixed take-profit and no time-based exit. A winner is ridden until
the trend turns, using the best **bid** reached since the entry filled as the
peak:

| Rule | Trigger |
| --- | --- |
| **Hard stop-loss** | Mark ≤ entry − `EXEC_STOP_LOSS_PCT` (0.7%). Evaluated first, always on. |
| **Trailing take-profit** | Armed once the peak reaches entry + `EXEC_TRAIL_ACTIVATION_PCT` (2%); closes when the mark retraces `EXEC_TRAIL_DISTANCE_PCT` (0.5%) from the peak. |

At activation the trail floor sits at peak − 0.5% ≈ +1.5%, always above the
−0.7% stop, so an armed position is closed in profit unless the book gaps
straight through the entry — and then the hard stop wins. Example: entry 1.00,
price runs to 1.05 (floor 1.04475); a tick at 1.0445 sells at the market for
+4.45%, while a further run to 1.08 keeps the position open with the floor at
1.0746.

```text
[EXEC] TRAIL ARMED LISTAUSDT peak=1.0200 (+2.00%) floor=1.014900 (+1.49%)
[EXEC] TRAIL PEAK LISTAUSDT peak=1.0500 (+5.00%) floor=1.044750 (+4.48%)
[EXEC] trailing stop hit LISTAUSDT: peak=1.0500 (+5.00%) mark=1.0445 (+4.45%)
[EXEC] placing MARKET SELL 191.7 LISTAUSDT @ 1.0443 — trailing-stop (pnl 4.45%)
[EXEC] exit fill 191.7 LISTAUSDT @ 1.0445 (fee 0.0400)
```

The peak is reset when the trade closes, so the next position starts from its own
entry.


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
| `EXEC_TRAIL_ACTIVATION_PCT` | 2 | Arm the trailing exit at +2% |
| `EXEC_TRAIL_DISTANCE_PCT` | 0.5 | Sell after a 0.5% retrace from the peak |
| `EXEC_STOP_LOSS_PCT` | 0.7 | Hard stop at -0.7% |
| `EXEC_COOLDOWN_SECS` | 10 | Pause between accepted entries |
| `EXEC_ORDER_TTL_SECS` | 12 | Cancel an unfilled maker entry after this many seconds |

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
SCAN_MAX_SPREAD_BPS=40
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
[EXEC] mode=paper pairs=12 order_notional=25USDT trail=2%/0.5% sl=0.7% gate_ttl=120s
[TICK] LISTAUSDT price=0.1302 latency=41ms
[DEPTH] FUNUSDT bid=0.001997 ask=0.002003
[AI] 2026-11-14 16:27:00 UTC asking DeepSeek about 12 pairs:
watchlist:
1. LISTAUSDT last=0.1302 bid=0.1301 ask=0.1302 spread=7.7bps chg24=30% amp24=40% vol24=4200000 vol1m=6120 volume_spike: yes anomaly=yes
...
account: equity=1000 cash=1000 position_symbol=- position_qty=0 ...
[AI] 2026-11-14 16:27:02 UTC decision: TRADE [LISTAUSDT, FUNUSDT, TRUMPUSDT] — momentum across the watchlist
[EXEC] AI gate TRADE 3 pairs: LISTAUSDT, FUNUSDT, TRUMPUSDT — momentum across the watchlist
[SIGNAL] LISTAUSDT BUY 0.1304 (1.21%) dip volume_spike=yes — >1% dip across 10 ticks with 1m volume 6120 USDT (spike; baseline 2916)
[EXEC] placing MAKER BUY 191.7 LISTAUSDT @ 0.1301 (signal: >1% dip across 10 ticks with 1m volume 6120 USDT (spike; baseline 2916))
[EXEC] order paper-1 accepted: submitted
[EXEC] portfolio pending BUY 191.7 LISTAUSDT @ 0.1301 (order paper-1)
[EXEC] entry fill 191.7 LISTAUSDT @ 0.1301 (fee 0.0499)
[EXEC] TRAIL ARMED LISTAUSDT peak=0.1328 (+2.075%) floor=0.132136 (+1.565%)
[EXEC] TRAIL PEAK LISTAUSDT peak=0.1352 (+3.920%) floor=0.134524 (+3.400%)
[EXEC] trailing stop hit LISTAUSDT: peak=0.1352 (+3.920%) mark=0.1345 (+3.382%)
[EXEC] placing MARKET SELL 191.7 LISTAUSDT @ 0.1344 — trailing-stop (pnl 3.382%)
[EXEC] exit fill 191.7 LISTAUSDT @ 0.1345 (fee 0.0516)
[EXEC] trade #1 closed: net 0.7420 USDT (1 of 1 winners)
```

## Safety notes

- Default mode is `paper`; live trading is an explicit opt-in.
- The AI can only open the pair it named, and only if that pair is on the
  scanner watchlist; a hallucinated ticker is rejected and logged.
- Order amounts are rounded **down** to the pair's base step, never above the
  configured notional.
- Reference prices for market orders are rounded **outward** to the pair's
  price step, never back inside the touch; maker prices are rounded **towards
  the passive side** and never cross.
- A dip without a volume spike is ignored: the strategy needs at least 2× the
  normal one-minute flow, not just a quiet price drift.
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

128 unit tests, no network required: Decimal anomaly windows, volume-spike
gating, scanner filtering and ranking, per-symbol books and watchlist pinning,
DeepSeek JSON
normalisation with multi-pair selection, HTX frame parsing/gzip/channel routing,
request signing against an independent golden vector, order/balance parsers,
portfolio PnL, every risk rejection, the paper broker's fill semantics and
symbol isolation, sizing/rounding helpers, startup balance recovery (primary
selection, dust filtering, unquoted assets, stop-loss arming) and full paper
round-trips through the multi-pair executor. The exit path is also covered for
the venue-minimum error that drops an unsellable residual instead of retrying.
