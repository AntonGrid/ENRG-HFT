# ENRG-HFT

Hybrid **multi-pair** trend-following bot — six layers, one binary.

| Layer | Module | Responsibility |
| --- | --- | --- |
| 0.5 Scanner | `src/scanner.rs` | Every `SCAN_INTERVAL_SECS` pulls `GET /market/tickers`, keeps liquid USDT spot pairs (24h quote volume ≥ `SCAN_MIN_QUOTE_VOLUME_USDT`), drops stablecoins and leveraged tokens, ranks the rest by 24h amplitude and reads per-pair `amount-precision` / `price-precision` from `/v1/common/symbols`. |
| 1. Fast hands | `src/exchange/htx.rs` | One HTX WebSocket (`wss://api.htx.com/ws`, fallback `wss://api.huobi.pro/ws`) for **all** scanned pairs: IPv4-only dial, gzip decoding, heartbeat, live `sub`/`unsub` as the watchlist rotates, per-symbol throttled logs, 15s stall watchdog and automatic reconnect. |
| 2. Mathematics | `src/quant_core.rs` | Exact `Decimal` indicators: OHLC bars, EMA, Wilder ATR and ADX (+DI/-DI) and a rolling VWAP. No `f64` anywhere, so a reading never drifts. |
| 2.5 Strategy | `src/strategy.rs` | `MarketBoard`: one isolated book per symbol fed by 1-minute bars aggregated from the trade stream. Trend filter = EMA 9/21/50 ribbon + Wilder ADX(14) >= 20 with +DI > -DI + price above the rolling VWAP. Entries are **long-only** `Signal { symbol, side: BUY, price, kind, adx, atr_pct, volume_spike }`: a Donchian-20 **breakout**, or a **pullback** that reclaims EMA 9 inside an intact uptrend. Volume is a conviction bonus, never a gate. |
| 3. Brain | `src/ai_brain.rs` | DeepSeek `deepseek-chat` risk manager with a strict `{"action":"TRADE"\|"PAUSE","symbols":["…"],"reason":"…"}` contract, polled every `AI_INTERVAL_SECS`. |
| 4. Muscles | `src/execution/` | Signed HTX REST client, risk guard, portfolio accounting, paper/live broker and the executor that places, tracks and closes orders **on any pair the AI cleared**. |

The AI is a **gate and a pair selector**, not a signal: a `TRADE` verdict opens
the gate for every symbol in its list for `EXEC_GATE_TTL_SECS`; the quant signal
on any of those symbols decides *when* to buy. The strategy is **long-only** and
follows trends in two ways:

- **Breakout** — price crosses above the highest high of the last `STRAT_BREAKOUT_BARS`
  (20) closed bars while the ribbon and the ADX gate agree;
- **Pullback** — inside an intact uptrend, a recent bar dipped to EMA 9 and price
  reclaims it (buy the discount, not the top).

Both entries need the full trend filter: `EMA9 > EMA21`, price above `EMA50`,
`ADX(14) >= 20` with `+DI > -DI`, and price above the rolling VWAP. A chopping or
falling market is therefore a no-trade: the bot buys continuation, never a
falling knife.

A `SELL` is never an entry signal: the executor only sells the position it
already holds (ATR trailing stop, hard stop-loss, trend flip or kill switch).
Volume never blocks an entry — a spike is recorded on the signal as a conviction
bonus — and one cooldown per symbol prevents a burst of back-to-back entries.

Bars of `STRAT_BAR_SECS` (60s) are aggregated from the trade stream and the
indicators only move on bar closes, so nothing is ever computed from a partially
formed bar. The first signal needs `warmup_bars` (50) closed bars, i.e. about 50
minutes of streaming; until then the board reports `trend=warming` and stays out
of the market.

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
                    Portfolio/PnL ──> ATR trail / hard SL / trend flip ──> exit order
```

- **Paper mode** (default) fills market orders at the live touch, rejects a
  post-only limit that would cross, and rests passive limits until the market
  trades through them. Fees are charged on both sides, so paper PnL is honest
  about spread and commissions.
- **Live mode** signs every request with HMAC-SHA256 (`StringToSign =
  METHOD\nhost\npath\nsorted-params`), sends POST parameters as an
  `application/json` body (only auth fields are signed), polls order state once
  a second, and logs every non-zero account balance at boot.
  Entries are split by pattern. A **breakout** is momentum, so it is sent as a
  `buy-market` taker order: the very move the signal describes is what would
  leave a passive bid behind. A **pullback** is bought into a dip, so it stays
  post-only (`buy-limit-maker`), resting `EXEC_MAKER_OFFSET_TICKS` steps above
  the best bid to sit at the front of the queue instead of its back. Both exits
  use `*-market` orders, because getting out matters more than the fee: the hard
  stop must fire at any cost, and a trailing stop that cannot fill is just a
  delayed stop-loss. An accepted maker entry immediately appears in the local
  `Portfolio` (`position_qty` / `avg_entry`, with paper cash reserved), so the AI
  context and risk guard never see a flat book while the order is still resting;
  a cancel or reject rolls the overlay back exactly. An unfilled entry is
  cancelled after `EXEC_ORDER_TTL_SECS`, or immediately when the market runs more
  than `EXEC_ENTRY_MAX_CHASE_BPS` above its limit (the signal no longer describes
  the price). Cancels go to `/v1/order/orders/{id}/submitcancel`; a failed cancel
  is retried on a 5s-per-attempt backoff capped at 60s, and after 10 attempts
  entries are halted so a stuck order is loud instead of a request loop. In live
  mode a cancel is followed by one last status query, so units that filled
  between the last poll and the cancel are still booked.
  The live USDT balance is mirrored into the AI context (`equity`/`cash`) every
  30 seconds and right after each fill; an entry is capped to the spendable
  balance so a small account is not rejected by the venue.
- **Balance audit** reconciles the venue with what the bot controls: at startup
  before the first signal, and again on every balance poll while flat. The
  scanner watchlist plays no part in it — a coin bought before a restart is
  exactly the coin a rotating top-12 list has dropped, so the decision is made
  from the balance sheet alone. With `EXEC_RECOVER_MODE=manage` (default) the
  largest non-USDT coin with a tradable USDT market is restored as the open
  position at the restart mark, pinned into the websocket feed and protected by
  the same trailing-stop / hard-stop exits; every other non-USDT balance
  (stablecoins, leveraged tokens, unrelated coins) is sold into USDT with a
  **market** order. `EXEC_RECOVER_MODE=liquidate` sells everything and stays
  flat. A balance the bulk ticker board does not carry is priced through
  `/market/detail/merged` before it is written off.
  Anything that is neither adopted nor sold — a balance whose sell the venue
  refused, a balance frozen by an order, an asset with no USDT market — is
  reported as `CRITICAL`/`WARNING` with its value in USDT, surfaced in the
  account line as `unmanaged_usdt=`, and (when priced) **blocks new entries**
  until it is gone: money without a stop must never be hidden behind a second
  position. The sell is retried on the next 30s poll. Balances worth less than
  HTX's 1 USDT minimum order are logged as `ignoring dust`: they can never be
  sold. If the ticker board itself is unreachable the audit reports the account
  as unverified and entries stay off rather than trade blind. The original cost
  basis is not persisted by HTX, so a recovered position is anchored to the
  current mark.
- **Ctrl-C** is a kill switch: the guard latches, working orders are cancelled,
  the position is flattened with one **market** order and a final report is
  printed.

## Exit scheme

There is no fixed take-profit and no time-based exit. Every exit is one of three
rules, evaluated in this order on every tick, using the best **bid** reached since
the entry filled as the peak:

| # | Rule | Trigger |
| --- | --- | --- |
| 1 | **Hard stop-loss** | Mark <= entry - `EXEC_STOP_LOSS_PCT` (0.7%). The emergency brake: always on and evaluated first. |
| 2 | **ATR trailing stop** (chandelier) | Mark <= peak - `EXEC_TRAIL_ATR_MULT` (2) x `ATR(14)`. Until the ATR of a symbol is warm the distance falls back to `EXEC_TRAIL_FALLBACK_PCT` (1.5%) of the peak. |
| 3 | **Trend flip** | The trend filter turns against the long (`EMA9 <= EMA21`, price below `EMA50` or `ADX < 20`). Disable with `EXEC_TREND_EXIT=false`. |

A trend follower has to let a winner run, so the trailing stop is the exit that
scales with the move. Because the distance is measured in ATRs, a quiet pair gets
a tight stop and a wild pair is not shaken out by its own noise. The fallback
percentage is deliberately *wider* than the hard stop, so an unwarmed ATR can
never make the emergency brake unreachable. Example: entry 1.00, ATR 0.0077
(2 x ATR = 0.0154), price runs to 1.05 -> stop at 1.0346; a tick at 1.034 sells at
the market, a further run to 1.08 lifts the stop to 1.0646.

```text
[EXEC] TRAIL PEAK LISTAUSDT peak=1.0200 (+2.00%) stop=1.004600 (+0.46%) atr=0.0077
[EXEC] TRAIL PEAK LISTAUSDT peak=1.0500 (+5.00%) stop=1.034600 (+3.46%) atr=0.0077
[EXEC] ATR trailing stop hit LISTAUSDT: peak=1.0500 (+5.00%) stop=1.0346 atr=0.0077 mark=1.0340 (+3.40%)
[EXEC] placing MARKET SELL 191.7 LISTAUSDT @ 0.1320 — atr-trailing-stop (pnl 3.688%)
[EXEC] exit fill 191.7 LISTAUSDT @ 1.0340 (fee 0.0400)
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
| `EXEC_TRAIL_ATR_MULT` | 2 | Trailing stop distance, in ATRs below the peak |
| `EXEC_TRAIL_FALLBACK_PCT` | 1.5 | Trail distance while the ATR is not warm yet |
| `EXEC_STOP_LOSS_PCT` | 0.7 | Hard stop at -0.7% |
| `EXEC_TREND_EXIT` | true | Close the position when the trend filter flips |
| `EXEC_COOLDOWN_SECS` | 10 | Pause between accepted entries |
| `EXEC_ORDER_TTL_SECS` | 30 | Cancel an unfilled entry after this many seconds |
| `EXEC_BREAKOUT_TAKER` | true | Send breakout entries as `buy-market` (false: post-only like a pullback) |
| `EXEC_MAKER_OFFSET_TICKS` | 1 | Venue steps above the bid a pullback entry rests at (never crossing) |
| `EXEC_ENTRY_MAX_CHASE_BPS` | 20 | Cancel a resting entry once the mark is this far above its limit (0 disables) |
| `EXEC_RECOVER_MODE` | manage | `manage`: adopt the largest balance and sell the rest; `liquidate`: sell every non-USDT balance and stay flat |

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
[BOOT] strategy: 60s bars, EMA 9/21/50, ADX(14) >= 20, ATR(14), Donchian(20), VWAP(30), warmup 50 bars, cooldown 30s
[EXEC] mode=paper pairs=12 order_notional=25USDT trail=2xATR sl=0.7% trend_exit=true gate_ttl=120s
[TICK] LISTAUSDT price=0.1302 latency=41ms
[DEPTH] FUNUSDT bid=0.001997 ask=0.002003
[AI] 2026-11-14 16:27:00 UTC asking DeepSeek about 12 pairs:
watchlist:
1. LISTAUSDT last=0.1302 spread=7.7bps chg24=30% amp24=40% vol24=4200000 vol1m=6120 spike=yes trend=up ema9=0.1298 ema21=0.1281 ema50=0.1262 vwap=0.1291 adx=31.4 +di=28.1 -di=12.6 atr=0.62% breakout=0.1302
...
account: equity=1000 cash=1000 position_symbol=- position_qty=0 ...
[AI] 2026-11-14 16:27:02 UTC decision: TRADE [LISTAUSDT, FUNUSDT, TRUMPUSDT] — momentum across the watchlist
[EXEC] AI gate TRADE 3 pairs: LISTAUSDT, FUNUSDT, TRUMPUSDT — momentum across the watchlist
[SIGNAL] LISTAUSDT BUY 0.1304 breakout adx=31.4 atr=0.62% volume_spike=yes — breakout above the 20-bar high 0.1302 with ema9 > ema21 and price above ema50, adx 31.4 (+DI 28.1 > -DI 12.6), atr 0.62% of price, price above vwap 0.1291, volume 6120 USDT/min (spike)
[EXEC] placing MAKER BUY 191.7 LISTAUSDT @ 0.1301 (signal: breakout above the 20-bar high 0.1302 ...)
[EXEC] order paper-1 accepted: submitted
[EXEC] portfolio pending BUY 191.7 LISTAUSDT @ 0.1301 (order paper-1)
[EXEC] entry fill 191.7 LISTAUSDT @ 0.1301 (fee 0.0499)
[EXEC] TRAIL PEAK LISTAUSDT peak=0.1328 (+2.075%) stop=0.129864 (+1.741%) atr=0.0014684
[EXEC] TRAIL PEAK LISTAUSDT peak=0.1352 (+3.920%) stop=0.1322632 (+3.583%) atr=0.0014684
[EXEC] ATR trailing stop hit LISTAUSDT: peak=0.1352 (+3.920%) stop=0.1322632 atr=0.0014684 mark=0.1322 (+3.688%)
[EXEC] placing MARKET SELL 191.7 LISTAUSDT @ 0.1320 — atr-trailing-stop (pnl 3.688%)
[EXEC] exit fill 191.7 LISTAUSDT @ 0.1322 (fee 0.0516)
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
- A trend is required, not just a price move: a breakout without the EMA
  ribbon, the ADX gate and the VWAP agreement is ignored, and a volume spike
  only raises the recorded conviction, never the permission.
- A missing or failing DeepSeek key leaves the gate closed: no entries, but the
  market feed, risk guard and positions remain live.
- An exit rejected with HTX `order-value-min-error` is logged once and the
  residual is dropped from local state, so the bot never spams the venue with an
  order that can never be filled.
- Cancels use the v1 `submitcancel` endpoint (a `/cancel` path answers 404 with
  an empty body, which no retry loop can distinguish from a network blip), are
  retried with a capped backoff and halt entries after 10 failures. An order the
  venue reports as non-existent is treated as terminal instead of being retried.
- A market buy is sent to HTX with its **quote** amount: the venue spends
  `amount` USDT on `buy-market`, not `amount` coins.
- The executor never blocks the socket: it reads only the books of its own
  working order / open position from the shared board and trades from its own
  task.

## Tests

```bash
cargo test
```

148 unit tests, no network required: Decimal EMA / Wilder / ATR / ADX / VWAP math,
volume-spike detection, scanner filtering and ranking, per-symbol bar aggregation,
the trend filter (breakout, pullback, chop, downtrend, warm-up, cooldown), watchlist
pinning, the exit policy (hard stop, ATR trail, trend flip, unwarmed fallback),
DeepSeek JSON normalisation with multi-pair selection, HTX frame parsing/gzip/channel
routing, request signing against an independent golden vector, order/balance parsers,
portfolio PnL, every risk rejection, the paper broker's fill semantics and symbol
isolation, sizing/rounding helpers, startup balance recovery (primary selection,
dust filtering, unquoted assets, stop-loss protection) and full paper round-trips
through the multi-pair executor. The balance audit is covered for the asset that
dropped out of the watchlist (it is still managed), for `EXEC_RECOVER_MODE=liquidate`,
for an unpriced balance (reported, not silently dropped) and for the entry block a
priced leftover raises, plus the per-symbol `/market/detail/merged` quote parser.
The exit path is also covered for the venue-minimum
error that drops an unsellable residual instead of retrying. Entry routing is pinned
too: breakout entries cross the spread, pullback entries rest one step above the bid
without ever crossing, and a resting entry is cancelled once the market runs more than
`EXEC_ENTRY_MAX_CHASE_BPS` above it. The cancel path itself is a golden-string test
(`/submitcancel`), and the retry schedule is covered without a venue (backoff growth,
cap, halt at the attempt limit, order-not-exist treated as terminal, market-buy quote
amount).
