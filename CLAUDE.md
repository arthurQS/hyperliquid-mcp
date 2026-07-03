# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

An MCP (Model Context Protocol) server exposing Hyperliquid perpetual-trading operations as tools over stdio, built on the official `hyperliquid-python-sdk`. Distributed on PyPI as `mcp-hyperliquid`; the console entry point is `hyperliquid-mcp` (`hyperliquid_mcp.server:main`).

## Commands

```bash
uv sync                                    # install deps (incl. dev extras)
uv run python -m hyperliquid_mcp.server    # run the server locally (stdio)
uv run pytest                              # run tests (tests/test_analytics.py covers the pure compute helpers)
uv run pytest path/to/test.py::test_name   # run a single test
uv run black src/                          # format
uv run mypy src/                           # type-check
```

The server needs `HYPERLIQUID_PRIVATE_KEY` in the environment to start (it raises on startup otherwise). Optional: `HYPERLIQUID_ACCOUNT_ADDRESS`, `HYPERLIQUID_TESTNET` (`"true"`/`"false"`), `HYPERLIQUID_VAULT_ADDRESS`. In production these come from the MCP client's `env` block, not a `.env` file.

**Agent mode.** When `HYPERLIQUID_ACCOUNT_ADDRESS` is set, the private key is treated as an *API/agent wallet* that signs on behalf of that main account — orders and queries act on `HYPERLIQUID_ACCOUNT_ADDRESS`, not on the key's own address. When unset, the key's own address is both signer and account. This is why handlers resolve `user_address = arguments.get("userAddress", self.account_address)` rather than deriving it from the key.

## Architecture

Everything lives in `src/hyperliquid_mcp/server.py` — a single `HyperliquidMCPServer` class. The whole system is essentially two SDK clients plus a big dispatch table:

- **`self.info`** (`hyperliquid.info.Info`) — read-only queries (positions, balances, order books, candles, funding). Constructed with `skip_ws=False` so the SDK's background WebSocket manager starts (see WebSocket state engine below). A throwaway bootstrap `Info` is still created with `skip_ws=True` just to discover perp dexes.
- **`self.exchange`** (`hyperliquid.exchange.Exchange`) — signed trading operations (place/cancel/modify orders). Signs with an `eth_account` `LocalAccount` derived from the private key. Must be constructed with the same `perp_dexs` as `self.info`, or its internal `name_to_coin` map can't resolve builder-dex coins and orders on them `KeyError`.

All SDK calls are blocking (`requests`-based). `_handle_tool_call` is therefore **synchronous** and `call_tool` dispatches it via `asyncio.to_thread` so a slow REST round-trip can't stall the MCP event loop — don't add `await`s inside `_handle_tool_call` or call it directly from the loop.

### WebSocket state engine

The server maintains a live in-memory mirror of the market fed by the SDK's WebSocket, so hot-path reads are O(1) local lookups instead of REST round-trips. The SDK's WebSocket is **thread-based** (`websocket-client`), not asyncio — callbacks fire on the WS I/O thread, so all shared state is guarded by `self._state_lock` (a `threading.Lock`).

- **`self.local_book`** — `coin -> {"levels", "time", "received_at"}`. Fed by `_on_l2_book`. Hyperliquid's `l2Book` pushes a full snapshot per message, so updating is just replacing the coin's entry (no delta merge).
- **`self.local_fills`** — a `deque(maxlen=200)` of recent live fills, fed by `_on_user_fills`. `userFills` is subscribed once account-wide at startup via `_start_streams()` (called from `run()`); not yet exposed by any tool.
- **`self.local_trades`** — `coin -> deque(maxlen=1000)` of executed market trades, fed by `_on_trades`. Unlike `l2Book` (full-snapshot replace), the `trades` stream **accumulates**, so the callback `extend`s the coin's deque. Subscribed lazily per-coin (`_ensure_trades_subscription`), like the book — NOT at startup. The callback is bound to the requested coin via a default-arg closure so the deque is keyed by that exact name (dodges any WS name remap / dex-prefix mismatch). `self._trades_last_recv` tracks per-coin last-message wall-clock for liveness.
- **`self._get_book(coin, stale_secs=2.0)`** — the shared fetch helper: lazily subscribes the coin's `l2Book` on first use (`_ensure_book_subscription`), returns the mirror when fresh (`< stale_secs` old) tagged `source="websocket"`, else falls back to a REST `l2_snapshot` tagged `source="rest"`. **REST fallback is mandatory** — the SDK's `run_forever()` does not auto-reconnect, so the socket can die silently; every book read must survive a dead/cold socket.
- **`self._get_trades(coin, window_secs, cold_secs=30.0)`** — the trade-flow fetch helper. Returns `(trades, source)` for trades within the window. Staleness differs from the book: a live market can be legitimately *silent*, so silence alone must not force REST. Falls back to REST `recentTrades` (via `_recent_trades_rest`) only on **cold start** (no WS data yet — handles the subscribe→data race) or when the window is empty AND no WS message arrived within `cold_secs` (the dead-socket guard). Otherwise serves the mirror; an empty window then correctly reads as "no flow", not a dead socket.
- `run()` calls `self.info.disconnect_websocket()` in a `finally` so the non-daemon WS/ping threads don't hang process exit.
- Reads/writes to `self.local_book` / `self.local_fills` / `self.local_trades` / `self._subscribed_books` / `self._subscribed_trades` / `self._trades_last_recv` must always happen under `self._state_lock`.

Tool wiring is a two-part pattern registered in `_register_handlers`:
1. `@server.list_tools()` returns static `Tool` schemas (the `hyperliquid_*` catalog).
2. `@server.call_tool()` wraps every call in try/except and delegates to `_handle_tool_call`, a long `if/elif` chain keyed on tool name. Errors are never raised to the client — they're serialized into the JSON `TextContent` response as `{"error": ...}`.

All responses are `json.dumps`'d dicts with a consistent shape: `message`, `data` (raw SDK payload), and usually a `summary` with derived fields. Responses are serialized **compact** (`separators=(",", ":")`, no indentation) in `call_tool` to roughly halve token cost. Exception: `hyperliquid_get_microstructure` and `hyperliquid_get_orderflow` intentionally return a flat dense dict (no `message`/`data`/`summary` wrapper) to stay ~20 tokens — don't "fix" them to match the convention.

### Key conventions to respect

- **Asset index ↔ coin name.** Trading tools (`place_order`, `place_bracket_order`) take an integer `asset` index; the SDK's `Exchange` methods want a coin *name*. `self.asset_index_to_name` (built by inverting `info.coin_to_asset`) does the resolution — unknown indices raise `ValueError`. Query tools that take `coin` accept the name directly.
- **Multi-dex support (HIP-3).** On init the server discovers all perp dexes via `info.perp_dexs()` and loads their universes (`perp_dexs=dex_names`) so builder-deployed assets (equities/commodities like `META`, `AAPL`, gold) resolve. Builder-dex global asset indices are offset by `110000 + i*10000` (i = 0-based position among non-default dexes). Query tools accept an optional `dex` param; cross-dex coins use dex-prefixed names (e.g. `"xyz:META"`).
- **Integer normalization.** `_handle_tool_call` coerces every param in `integer_params` (currently `asset`, `oid`, `startTime`, `endTime`, `twapId`, `minutes`, `depth`, `window_secs`, `days_forward`, `iterations`, `lookback_days`) via `int(float(x))` before dispatch — clients sometimes send floats. Add any new int param to that list.
- **Prices/sizes as strings.** Tool schemas take prices and sizes as strings (to avoid float precision loss over the wire); handlers convert to `float` for the SDK.
- **Market orders.** Hyperliquid has no native market orders — a resting buy limit at 0 would never fill. Price `"0"` (in `place_order` for non-trigger orders, and for the bracket entry) is emulated like the SDK's `market_open`: an aggressive IoC limit at `exchange._slippage_price(coin, is_buy, Exchange.DEFAULT_SLIPPAGE)` (mid ± 5%).
- **Bracket orders** are placed atomically via `exchange.bulk_orders` — entry + TP + SL, where TP/SL are opposite-side reduce-only trigger orders. The TP rests as a limit at its price; the SL triggers as **market** (`isMarket: True`) with a slippage-bounded `limit_px` (±5% around the trigger) — a limit SL can gap through its price and never fill, defeating the stop. Response statuses map positionally to `["entry", "take-profit", "stop-loss"]`.
- **Raw `info.post("/info", ...)` calls** (e.g. `recentTrades`, used by `get_recent_trades` and `_recent_trades_rest`) must remap the coin via `self.info.name_to_coin.get(coin, coin)` first — proper Info methods do this internally, raw posts don't.
- **TWAP tools** (`place_twap_order`, `cancel_twap_order`) are declared in the schema but raise `NotImplementedError`.
- **Order book depth & microstructure.** `get_order_book` takes an optional `depth` (default 5) and truncates to the top-N levels per side (liquidity is power-law; the immediate spread carries most of the signal, and truncating saves tokens). `get_microstructure` computes edge-level features server-side from the same mirror: `OBI` (bid share of top-`depth` volume, >0.5 = upward pressure), Stoikov `micro_price` (each side's price weighted by the opposite side's top size; top-of-book), `mid`, and `spread_bps`. Both go through `_get_book` and carry a `source` field (`"websocket"`/`"rest"`) so the caller knows freshness. Computation lives in the `_microstructure` staticmethod.
- **Trade flow.** `get_orderflow` is the trade-flow analog of OBI: it reads the executed-trade tape (via `_get_trades`) over a `window_secs` lookback (default 60) and computes signed aggressor volume (`buy_vol`/`sell_vol`), `CVD` (= buy − sell), `TFI` (trade-flow imbalance in [-1,1], >0 = net buying), plus `trades`, `vwap`, `last_px`, `duration_s`. Aggressor side comes from each trade's `side` field: `"B"` = buy aggressor, `"A"` = sell aggressor (verified against the SDK's ccxt `parse_trade`). Computation lives in the `_orderflow` staticmethod; returns `None` (→ `{"error": "no trades in window", ...}`) on an empty window, mirroring `_microstructure` on an empty book.
- **Monte Carlo risk (`run_monte_carlo`).** `hyperliquid_run_monte_carlo` is the one heavy-compute tool and the only reason `numpy` is a dependency. It fetches recent candles (`candles_snapshot`, default `1h`/`lookback_days=30`), estimates per-step volatility `sigma` (and optionally the per-step mean log return `m`) from log returns, and samples **terminal prices directly**: since only terminal stats are reported and a sum of iid normal log increments is itself normal, `log S_T = log s0 + steps*m + sigma*sqrt(steps)*Z` — one draw per path, O(iterations) time/memory regardless of horizon, distributionally identical to stepping full paths (don't "restore" the per-step cumsum; it was an O(iterations×steps) memory blowup). **Drift semantics:** the observed mean log return already embeds GBM's Itô correction (`E[log ret] = mu − sigma²/2`), so with `use_historical_drift=True` it's used as-is — never subtract `sigma²/2` again. The default "zero drift" (`use_historical_drift=False`) means a **martingale price** (`m = −sigma²/2`, so `E[S_T] = s0`, zero expected *return*) — conservative for risk/VaR and free of short-sample directional bias. Returns the aggregated risk profile (terminal-price percentiles, `expected_return`, 5% `VaR_5pct` as a positive loss magnitude, `prob_profit`). `steps = days_forward * steps_per_day[interval]` keeps the horizon and estimation interval consistent; `s0` is the last close; `iterations` (default 10000) is capped at 1M. Compute lives in the `_monte_carlo` staticmethod (returns `None` → `{"error": ...}` on <3 closes, non-positive prices, or zero/non-finite vol); it takes an optional `rng` (`np.random.Generator`) for deterministic tests, unseeded by default. Output is a flat dense dict like microstructure/orderflow, not the `message`/`data`/`summary` wrapper.

## Adding a tool

1. Append a `Tool(...)` schema in `list_tools()`.
2. Add an `elif name == "..."` branch in `_handle_tool_call` returning the `message`/`data`/`summary` dict shape.
3. If it takes new integer params, add them to `integer_params`.
