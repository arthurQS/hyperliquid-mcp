# hyperliquid-mcp

An MCP server that gives your AI direct, signed access to Hyperliquid perps. Built on the official Python SDK, hardened for the one failure mode that actually costs money: a rejected order reported as success. Every write parses the exchange response before it claims anything. Every read tells you where the data came from.

You point Claude (or any MCP client) at it, hand it a key, and it trades.

## Quick start

```bash
uvx --from mcp-hyperliquid hyperliquid-mcp
```

That's it. No `.env` file, no config directory — everything comes from your MCP client's `env` block:

```json
{
  "mcpServers": {
    "hyperliquid": {
      "command": "uvx",
      "args": ["--from", "mcp-hyperliquid", "hyperliquid-mcp"],
      "env": {
        "HYPERLIQUID_PRIVATE_KEY": "0x...",
        "HYPERLIQUID_TESTNET": "true"
      }
    }
  }
}
```

Yes, `"true"`. Start on testnet. Flip it to `"false"` after your strategy survives fake money.

Running from source instead:

```bash
git clone https://github.com/Dakkshin/hyperliquid-mcp.git
cd hyperliquid-mcp
uv sync
uv run python -m hyperliquid_mcp.server
```

(Same client config, but `command: "uv"`, `args: ["--directory", "/path/to/hyperliquid-mcp", "run", "python", "-m", "hyperliquid_mcp.server"]`.)

## Before it will trade

Hyperliquid doesn't know your wallet until it holds funds. One-time setup:

- **Mainnet:** connect at https://app.hyperliquid.xyz, deposit anything from Arbitrum. Deposit = registration.
- **Testnet:** connect at https://app.hyperliquid-testnet.xyz, hit the faucet.

Skip this and every write comes back `User or API Wallet does not exist`. The server will hand you that exact message instead of a stack trace — but it can't deposit for you.

## Pick your setup

**Just exploring / paper trading.** `HYPERLIQUID_TESTNET=true`, your wallet key, done. Testnet is a full parallel universe: separate ledger, separate agent approvals, and — this bites everyone — **separate asset indices**. BTC is index 0 on mainnet and 3 on testnet. Never hardcode an index; resolve through `hyperliquid_get_meta` every time.

**Running real money.** Use agent mode. Generate an API wallet in the Hyperliquid UI (More -> API), approve it from your main account, then:

```json
"env": {
  "HYPERLIQUID_PRIVATE_KEY": "0xApiWalletKey...",
  "HYPERLIQUID_ACCOUNT_ADDRESS": "0xYourMainAccount...",
  "HYPERLIQUID_TESTNET": "false"
}
```

The API wallet signs; your main key never touches the machine running the model. This is the only sane production configuration. Agent approvals are per-network — a mainnet-approved agent is a stranger on testnet.

**Trading builder-dex perps (equities, gold, HIP-3 stuff).** Set `HYPERLIQUID_PERP_DEXS="xyz"` (comma-separated for several). Unset means primary dex only — ~2s startup, covers every standard perp. `"all"` loads every discovered dex: hundreds of serial REST calls, ~90s, and your MCP client kills the connection at 30s unless you raise `MCP_TIMEOUT=180000`. You almost certainly don't want `"all"`.

Optional: `HYPERLIQUID_VAULT_ADDRESS` if you trade a vault.

## What it does

Thirty tools. The interesting ones:

**Execution.** `place_order`, `place_bracket_order`, `modify_order`, `cancel_order`, `cancel_all_orders`, `update_leverage`. Market orders are emulated the way the SDK does it — aggressive IoC limit at mid ±5% — because Hyperliquid has no native market order and a resting buy at $0 would just sit there forever.

**Brackets are real OCO.** Entry + TP + SL ship as one atomic `normalTpsl` group. TP and SL are children of the entry: one fills, the exchange kills the other; cancel the entry, both die. No stale stop left resting on the book at 3am. The stop-loss triggers as *market* with a slippage-bounded limit — a limit stop can gap straight through its price and never fill, which defeats the entire point of a stop.

**The server refuses garbage before signing it.** Wrong-side stop on a long (SL above entry)? Rejected locally — it would trigger the instant it hit the book. `size: "NaN"`? Rejected — the SDK's wire encoder would happily sign it otherwise. `asset: 5.7`? Rejected, not truncated to 5 — silent truncation means ordering the wrong instrument.

**And it never lies about outcomes.** A rejected cancel says `Cancel failed` with the exchange's reason. A bracket with an invalid leg reports the atomic group rejection. `cancel_all_orders` returns per-oid outcomes and an honest count — including the untriggered TP/SL stops, which the naive open-orders endpoint doesn't even list. If a write *times out*, the response says "may still have executed, recheck" — because a timeout after the request left the building is not a failure, and treating it as one is how you double-fill.

**Market data with a freshness receipt.** Order books, trades, candles, funding, open interest. Hot reads are served from a live in-memory WebSocket mirror — O(1), no REST round-trip — and every response carries `source: "websocket"` or `"rest"` so you know exactly what you got. The SDK's socket doesn't auto-reconnect; when it dies, reads silently degrade to REST instead of serving you a stale book. Slower, never wrong.

**Server-side quant, token-sized payloads.** The whole point of computing on the server: an 8B model shouldn't be doing float comparisons, and you shouldn't pay tokens for 1000 raw book levels.

- `get_microstructure` — OBI, Stoikov micro-price, spread in bps. ~20 tokens.
- `get_orderflow` — CVD, aggressor buy/sell volume, trade-flow imbalance off the live tape. Book says intent, tape says action; read both.
- `get_indicators` — RSI (Wilder's, pinned by tests against Cutler's), Bollinger (population std, also pinned), EMA 9/21/50/200, volume SMA. One call, one interval. Returns raw values plus *deterministic* flags — `rsi.zone`, `ema.is_ordered_up`, `bollinger.is_squeeze`. Mathematical facts only. It will never tell you `signal: "BUY"`; forming opinions is your model's job.
- `run_monte_carlo` — vectorized GBM, 10k+ paths, returns VaR, terminal percentiles, probability of profit. Samples terminal prices directly (one draw per path, O(iterations) whatever the horizon). Default drift is martingale — zero expected return — because 30 days of history is a vibe, not a drift estimate. Check `observations` in the output: the candle API caps around 5000 rows, so a 30-day lookback at 1m candles is quietly ~3.5 days of data. The field exists so you can catch it.
- `get_beta` — beta, correlation, R² against a benchmark you must name (no sneaky default). Timestamp-aligned log-return regression. No alpha, no flags — beta is a continuous sizing input, and a `beta > 1` boolean would invent a cliff that doesn't exist.

**Not implemented:** `place_twap_order` / `cancel_twap_order` are schema stubs that raise. They're declared so clients can see them coming; they don't work yet. ¯\\_(ツ)_/¯

## Talking to it

You don't call these tools — your model does. Prompts that work:

```
Show me my Hyperliquid balance
```

Returns both ledgers — perp *and* spot. Hyperliquid keeps them separate, and a wallet holding only spot USDC reports a $0 perp balance. Learned that one the annoying way; the tool now surfaces `totalUsdcAcrossAccounts` so nobody else has to.

```
Is there buy or sell pressure on HYPE right now?
```

One `get_microstructure` call:

```json
{"asset":"HYPE","source":"websocket","depth":5,"OBI":0.62,"micro_price":1.452,"mid":1.451,"spread_bps":2.1}
```

OBI 0.62 = bids carrying 62% of top-of-book volume. Micro-price above mid = pressure pointing up. Spread ~2bps = liquid.

```
Long 4.12 SOL, entry 218, target 219.50, stop 216.80
```

Model resolves SOL's index via `get_meta`, fires `place_bracket_order`, gets back three legs with statuses. If it had asked for a stop above entry, the order never leaves the machine.

```
If I hold BTC for 7 days, what's my downside?
```

`run_monte_carlo` -> `VaR_5pct: 0.0836` = 8.4% loss at the 5th percentile. Ten thousand simulated paths, none of which cross the wire — only the summary does.

## When it breaks

**`User or API Wallet does not exist`** — wallet not registered on *this* network, or agent not approved on *this* network. Deposit/faucet first; approve agents per-network.

**`Order has invalid price.` / `Order has invalid size.`** — tick and lot rules. Prices: max 5 significant figures and per-asset decimal limits. Sizes: `szDecimals` from `get_meta`. And `size × price ≥ $10`, always.

**Connection times out at startup** — you set `HYPERLIQUID_PERP_DEXS="all"`. Unset it, or raise `MCP_TIMEOUT`. This is a startup-speed problem, not a config error.

**Every read says `source: "rest"`** — the WebSocket died (it doesn't reconnect) or hasn't warmed up yet. First read on a coin is always REST; the mirror takes over within a couple of seconds. Persistent REST means restart the server. Data stays correct either way — that's the fallback doing its job.

**Debugging anything else:**

```bash
HYPERLIQUID_LOG_LEVEL=DEBUG uvx --from mcp-hyperliquid hyperliquid-mcp
```

## Security, bluntly

1. The key in your MCP config can sign orders. Treat that file like the key itself.
2. Agent mode for anything real. Main key stays offline.
3. Testnet first. The parallel universe is free.
4. Brackets over naked entries. The exchange-side OCO exists so a dead process can't orphan your stop.
5. The server never logs or echoes the private key. It also can't stop you from pasting it into a chat. Don't.

## Development

```bash
uv sync
uv run pytest        # ~100 tests: golden-value math pins, response-parsing fixtures, liveness guards
uv run black src/
uv run mypy src/
```

Everything lives in `src/hyperliquid_mcp/server.py` — one class, two SDK clients, a dispatch table, and a threaded WebSocket state engine. The tests are the contract: Wilder RSI vs Cutler's, Bollinger `ddof=0` vs `ddof=1`, martingale drift, OCO status parsing — swap a formula and the suite fails loudly, which is the entire point.

PRs welcome. Bring tests; the math ones especially — a wrong number that parses is worse than a crash.

## Community

- [Telegram](https://t.me/+fC8GWO3zBe04NTY0) — strategies, help, war stories.

## Resources

- [Hyperliquid docs](https://hyperliquid.gitbook.io/)
- [hyperliquid-python-sdk](https://github.com/hyperliquid-dex/hyperliquid-python-sdk)
- [Model Context Protocol](https://modelcontextprotocol.io/)

## License

MIT.

## Disclaimer

This is software that lets a language model sign orders against a perpetuals exchange with real leverage. It is provided as-is, it has edge cases, and markets do not care about you. Trade only what you can lose entirely. Nobody here is responsible for your P&L.
