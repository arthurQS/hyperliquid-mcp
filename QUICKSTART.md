# Quickstart

Zero to signed order in about five minutes. The long version lives in [README.md](README.md); this is the short one.

## 0. Prereqs (30 seconds)

```bash
python --version   # need 3.10+
pip install uv     # if you don't have uv/uvx yet
```

## 1. Register your wallet (2 minutes)

Hyperliquid doesn't know you exist until you deposit. Pick a lane:

- **Testnet (start here):** https://app.hyperliquid-testnet.xyz -> connect wallet -> faucet. Free money, real order book.
- **Mainnet:** https://app.hyperliquid.xyz -> connect -> deposit anything from Arbitrum. $10 is plenty; the deposit itself is the registration.

Checkpoint: the Hyperliquid UI shows a balance. No balance, no trading — every write will bounce with `User or API Wallet does not exist` until this is done.

## 2. Wire up your MCP client (1 minute)

Add this to your client's MCP config. For Claude Desktop that's:

- **Mac:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
- **Claude Code:** `claude mcp add-json hyperliquid '<the json below>'`

```json
{
  "mcpServers": {
    "hyperliquid": {
      "command": "uvx",
      "args": ["--from", "mcp-hyperliquid", "hyperliquid-mcp"],
      "env": {
        "HYPERLIQUID_PRIVATE_KEY": "0xYOUR_KEY",
        "HYPERLIQUID_TESTNET": "true"
      }
    }
  }
}
```

Two things, no negotiation:

1. That key can sign orders. The config file is now key material — keep it out of git, screenshots, and pastebins.
2. There is no `.env` file. The server reads its environment from this `env` block and nowhere else.

Running from a local clone instead of PyPI:

```json
"command": "uv",
"args": ["--directory", "/path/to/hyperliquid-mcp", "run", "python", "-m", "hyperliquid_mcp.server"]
```

Checkpoint: config saved, JSON actually valid (trailing commas kill more MCP setups than anything else).

## 3. Restart the client (30 seconds)

Fully quit — kill the process, not the window — and reopen. Then confirm the server is listed: Claude Desktop shows connected MCP servers in the tools menu; Claude Code shows them under `/mcp`.

If it's missing: bad JSON, or the key doesn't start with `0x`. That's 95% of failures right there.

## 4. Prove it works (30 seconds)

Say these to your model, in order:

```
Show me my Hyperliquid balance
```

Expect perp AND spot balances. They're separate ledgers — money in spot shows $0 on the perp side until transferred. Not a bug.

```
What are BTC, ETH, and SOL trading at on Hyperliquid?
```

Expect three live mid prices.

```
Show me tradeable assets on Hyperliquid with their indices
```

Expect the full universe from `get_meta`. Do not memorize these numbers: **indices differ per network** — BTC is 0 on mainnet and 3 on testnet. The model resolves them per call; let it.

All three answered? You're live.

## 5. First order (1 minute, optional but do it)

```
Place a limit buy on Hyperliquid: 0.05 SOL, priced 10% below the current
market so it rests without filling. We'll cancel it right after.
```

Expect an order ID and status `resting`. Then:

```
Cancel all my open orders on Hyperliquid
```

Expect an honest per-order outcome — `cancelledCount: 1`, not vibes. If a cancel ever fails, the response says `Cancel failed` with the exchange's reason. This server does not report success it didn't get.

You placed and killed an order. The write path works. ¯\\_(ツ)_/¯

## When it doesn't work

**`User or API Wallet does not exist`** — Step 1 skipped or incomplete, or (agent mode) the API wallet isn't approved on *this* network. Testnet and mainnet approvals are completely separate.

**Server not listed after restart** — JSON syntax. Validate the file. Then check the key starts with `0x`. Then actually quit the client, not just the window.

**`Order value must be at least $10`** — exchange minimum: `size x price >= $10`. 0.05 SOL at $200 clears it; 0.01 doesn't.

**Startup times out** — you set `HYPERLIQUID_PERP_DEXS="all"` somewhere. Unset it. Details in the README.

**Old Python** — need 3.10+. `brew install python@3.11` / python.org / your distro's repo.

## Where to next

- **Bracket orders** — "Long 2 SOL at 218, target 221, stop 216." Entry + TP + SL as one atomic exchange-side OCO: one side fills, the other auto-cancels. Wrong-side stop? Rejected locally before anything is signed. This is the tool that keeps a dead process from orphaning your stop — use it over naked entries.
- **Market pressure** — "Any buy pressure on HYPE right now?" gets OBI, micro-price, and spread in a ~20-token payload.
- **Risk** — "What's my 7-day downside on BTC?" runs a 10k-path Monte Carlo server-side and hands back VaR.
- **Everything else** — [README.md](README.md) covers all thirty tools, agent mode for production keys, and the failure modes worth knowing.

## Rules for real money

1. Testnet until your process is boring.
2. Agent mode on mainnet — API wallet signs, main key stays offline.
3. Brackets, always. A position without a stop is a donation with extra steps.
4. Small sizes first. Prove cancellation works before you scale.
5. Markets don't care about you. Size accordingly.
