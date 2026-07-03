# Hyperliquid MCP Server

A Model Context Protocol (MCP) server for Hyperliquid perpetual trading using the official Python SDK. This server provides AI assistants with secure, reliable access to Hyperliquid's trading platform.

## Features

✅ **Official SDK** - Built on the official Hyperliquid Python SDK with proper signing  
✅ **Complete Coverage** - All trading endpoints: orders, positions, market data, vaults  
✅ **Secure** - Proper EIP-712 signing with agent mode support  
✅ **Bracket Orders** - Atomic entry + TP + SL order placement  
✅ **Market Data** - Real-time prices, order books, funding rates, candles  
✅ **Live WebSocket State Engine** - In-memory order book mirror for instant, low-latency reads  
✅ **Microstructure Signals** - Server-computed OBI, micro-price, and spread (bps) in a ~20-token payload  
✅ **Trade-Flow Signals** - Server-computed CVD, aggressor volume, and trade-flow imbalance from the live trade tape  
✅ **Monte Carlo Risk** - Vectorized GBM simulation (10k+ paths) returning VaR, terminal distribution, and probability of profit  
✅ **Account Management** - Positions, balances, fills, funding history  
✅ **Testnet Support** - Test strategies safely before going live

## Prerequisites

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) or [uvx](https://github.com/astral-sh/uv) for package management
- A Hyperliquid account with deposited funds

## Installation

### Using uvx (Recommended)

```bash
# Install and run directly from PyPI
uvx --from mcp-hyperliquid hyperliquid-mcp
```

### Using pip

```bash
# Install with pip
pip install mcp-hyperliquid

# Run
mcp-hyperliquid
```

### Local Development

```bash
# Clone and install from source
git clone https://github.com/Dakkshin/hyperliquid-mcp.git
cd hyperliquid-mcp
uv sync

# Run locally
uv run python -m hyperliquid_mcp.server
```

#### Local Development Configuration

If you're running from source code locally, use this configuration:

```json
{
  "mcpServers": {
    "hyperliquid": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/hyperliquid-mcp",
        "run",
        "python",
        "-m",
        "hyperliquid_mcp.server"
      ],
      "env": {
        "HYPERLIQUID_PRIVATE_KEY": "0x1234567890abcdef...",
        "HYPERLIQUID_TESTNET": "false"
      }
    }
  }
}
```

Replace `/path/to/hyperliquid-mcp` with the actual path to your cloned repository.

## Configuration

### 1. Register Your Wallet on Hyperliquid

**IMPORTANT:** Your wallet must be registered on Hyperliquid before trading.

**Mainnet:**

1. Go to https://app.hyperliquid.xyz
2. Connect your wallet
3. Deposit funds from Arbitrum One (any amount registers your wallet)

**Testnet:**

1. Go to https://app.hyperliquid-testnet.xyz
2. Connect your wallet
3. Get testnet funds from the faucet or bridge

### 2. Configure Your MCP Client

Environment variables are now configured directly in your MCP client settings (no `.env` file needed).

#### Claude Desktop / Kiro

Add to your `mcp.json` configuration file:

```json
{
  "mcpServers": {
    "hyperliquid": {
      "command": "uvx",
      "args": ["--from", "mcp-hyperliquid", "hyperliquid-mcp"],
      "env": {
        "HYPERLIQUID_PRIVATE_KEY": "0x1234567890abcdef...",
        "HYPERLIQUID_TESTNET": "false"
      }
    }
  }
}
```

**Required Environment Variables:**

- `HYPERLIQUID_PRIVATE_KEY` - Your wallet's private key for signing transactions

**Optional Environment Variables:**

- `HYPERLIQUID_ACCOUNT_ADDRESS` - For agent/API wallet mode (advanced)
- `HYPERLIQUID_TESTNET` - Set to "true" for testnet, "false" or omit for mainnet
- `HYPERLIQUID_VAULT_ADDRESS` - For vault trading

#### Full Configuration Example

```json
{
  "mcpServers": {
    "hyperliquid": {
      "command": "uvx",
      "args": ["--from", "mcp-hyperliquid", "hyperliquid-mcp"],
      "env": {
        "HYPERLIQUID_PRIVATE_KEY": "0x1234567890abcdef...",
        "HYPERLIQUID_ACCOUNT_ADDRESS": "0xYourTradingAccountAddress...",
        "HYPERLIQUID_TESTNET": "false",
        "HYPERLIQUID_VAULT_ADDRESS": "0xVaultAddress..."
      }
    }
  }
}
```

#### Other MCP Clients

Configure according to your client's documentation, using:

- **Command:** `uvx` or `python`
- **Args:** `["--from", "mcp-hyperliquid", "hyperliquid-mcp"]` or `["-m", "hyperliquid_mcp.server"]`
- **Environment:** Add the required environment variables in your client's env configuration

## Available Tools

### Account & Position Management

- **`hyperliquid_get_account_info`** - Get complete account summary
- **`hyperliquid_get_positions`** - Get all open positions
- **`hyperliquid_get_balance`** - Get account balance and withdrawable amount

### Order Management

- **`hyperliquid_place_order`** - Place a single order
- **`hyperliquid_place_bracket_order`** - Place entry + TP + SL atomically
- **`hyperliquid_cancel_order`** - Cancel a specific order
- **`hyperliquid_cancel_all_orders`** - Cancel all open orders
- **`hyperliquid_modify_order`** - Modify an existing order
- **`hyperliquid_place_twap_order`** - Place TWAP order (coming soon)
- **`hyperliquid_cancel_twap_order`** - Cancel TWAP order (coming soon)

### Order Queries

- **`hyperliquid_get_open_orders`** - Get all open orders
- **`hyperliquid_get_order_status`** - Get status of specific order
- **`hyperliquid_get_user_fills`** - Get trade fill history
- **`hyperliquid_get_user_funding`** - Get funding payment history

### Market Data

- **`hyperliquid_get_meta`** - Get exchange metadata (assets, leverage, etc.)
- **`hyperliquid_get_all_mids`** - Get current mid prices for all assets
- **`hyperliquid_get_order_book`** - Get order book depth (top `depth` levels per side, default 5; served from a live WebSocket mirror)
- **`hyperliquid_get_microstructure`** - Get dense edge-level signals (OBI, micro-price, spread in bps) computed server-side
- **`hyperliquid_get_orderflow`** - Get dense trade-flow signals (CVD, aggressor buy/sell volume, trade-flow imbalance) over a recent window, computed server-side
- **`hyperliquid_get_recent_trades`** - Get recent trades
- **`hyperliquid_get_historical_funding`** - Get funding rate history
- **`hyperliquid_get_candles`** - Get OHLCV candle data

### Risk & Quant

- **`hyperliquid_run_monte_carlo`** - Run a vectorized GBM Monte Carlo price simulation and return an aggregated risk profile (VaR, terminal-price distribution, probability of profit), computed server-side

### Vault Management

- **`hyperliquid_vault_details`** - Get vault details
- **`hyperliquid_vault_performance`** - Get vault performance metrics

### Utility

- **`hyperliquid_get_server_time`** - Get server timestamp

## Usage Examples

### Example 1: Check Account Balance

```
Show me my Hyperliquid account balance
```

The AI will call `hyperliquid_get_balance` and show you:

- Account value
- Margin used
- Withdrawable amount
- Available balance

### Example 2: Get Market Data

```
What's the current price of SOL on Hyperliquid? Show me the order book too.
```

The AI will:

1. Call `hyperliquid_get_meta` to find SOL's index
2. Call `hyperliquid_get_all_mids` for current price
3. Call `hyperliquid_get_order_book` for depth

By default `hyperliquid_get_order_book` returns the top **5** levels per side (pass `depth` for more). The book is served instantly from a live WebSocket mirror when fresh, and each response carries a `source` field (`"websocket"` or `"rest"`) so you know how fresh it is.

### Example 3: Gauge Market Pressure (Microstructure)

```
Is there buy or sell pressure on HYPE right now, and how tight is the spread?
```

The AI calls `hyperliquid_get_microstructure` and gets back a dense signal computed server-side (no raw book parsing needed):

```json
{"asset":"HYPE","source":"websocket","depth":5,"OBI":0.62,"micro_price":1.452,"mid":1.451,"spread_bps":2.1}
```

- **`OBI`** (Order Book Imbalance) — bid share of top-`depth` volume; `>0.5` means bids are heavier (upward pressure).
- **`micro_price`** — Stoikov fair value that reacts faster than the plain mid.
- **`spread_bps`** — bid-ask spread in basis points (tightening ≈ liquid/imminent move, widening ≈ thin).

This is far cheaper on tokens than fetching the raw order book — prefer it when you only need to read market pressure rather than inspect individual levels.

### Example 4: Read Trade Flow (Order Flow / CVD)

```
Over the last minute, is BTC seeing net buying or selling on Hyperliquid?
```

The AI calls `hyperliquid_get_orderflow` (optionally with `window_secs`, default 60) and gets a dense signal computed server-side from the executed-trade tape:

```json
{"asset":"BTC","source":"websocket","window_secs":60,"buy_vol":12.34,"sell_vol":9.87,"CVD":2.47,"TFI":0.111,"trades":143,"vwap":61234.5,"last_px":61240.0,"duration_s":59.8}
```

- **`buy_vol` / `sell_vol`** — aggressor-signed volume (`"B"` = buy aggressor, `"A"` = sell aggressor).
- **`CVD`** — cumulative volume delta over the window (`buy_vol − sell_vol`); positive = net buying.
- **`TFI`** — trade-flow imbalance in `[-1, 1]` (0 = balanced); the flow analog of OBI.
- **`vwap` / `last_px` / `trades` / `duration_s`** — window VWAP, last print, trade count, and actual span covered.

Where `get_microstructure` reads the *resting book*, `get_orderflow` reads *executed flow* — use them together to see both intent and action.

### Example 5: Simulate Downside Risk (Monte Carlo)

```
If I hold BTC for the next 7 days, what's my downside risk?
```

The AI calls `hyperliquid_run_monte_carlo` (defaults: `1h` candles, `lookback_days=30`, `days_forward=7`, `iterations=10000`). The server estimates volatility from recent candles, runs the paths through a vectorized Geometric Brownian Motion model in numpy, and returns only the aggregated risk profile:

```json
{"asset":"BTC","interval":"1h","days_forward":7,"lookback_days":30,"drift":"zero","s0":61240.0,"steps":168,"iterations":10000,"sigma_per_step":0.0031,"mu_per_step":0.0,"mean_terminal":61230.4,"median_terminal":61180.2,"p05_terminal":56120.7,"p95_terminal":66540.9,"expected_return":-0.0002,"VaR_5pct":0.0836,"prob_profit":0.497}
```

- **`VaR_5pct`** — 5% Value at Risk as a positive loss magnitude (here ≈ 8.4% worst-case over the horizon at the 5th percentile).
- **`p05_terminal` / `median_terminal` / `p95_terminal`** — terminal-price percentiles bracketing the outcome distribution.
- **`prob_profit`** — share of paths ending above the current price.
- **`drift`** — `"zero"` by default (conservative for risk); pass `use_historical_drift=true` to use the historical mean return as drift.

Only the summary crosses the wire — the thousands of simulated paths never leave the server, so it stays cheap on tokens while doing the heavy compute in Python.

### Example 6: Place a Bracket Order

```
Place a bracket order on Hyperliquid:
- Pair: SOL-USD
- Side: BUY (LONG)
- Size: 4.12 SOL (~$900)
- Entry: $218.00
- Target: $219.50 (+0.7%)
- Stop Loss: $216.80 (-0.8%)
```

The AI will:

1. Call `hyperliquid_get_meta` to get SOL's asset index (5)
2. Call `hyperliquid_place_bracket_order` with:
   - asset: 5
   - isBuy: true
   - size: "4.12"
   - entryPrice: "218.00"
   - takeProfitPrice: "219.50"
   - stopLossPrice: "216.80"

This places 3 orders atomically:

- Entry order at $218.00
- Take profit trigger at $219.50 (reduce-only)
- Stop loss trigger at $216.80 (reduce-only)

### Example 7: Check Positions and Close

```
Show me my open positions. If I have a SOL position, close it at market price.
```

The AI will:

1. Call `hyperliquid_get_positions`
2. If SOL position exists, call `hyperliquid_place_order` with:
   - Opposite side (sell if long, buy if short)
   - Market order (price = "0")
   - Reduce-only enabled

### Example 8: View Recent Trading Activity

```
Show me my last 50 trades from the past 24 hours
```

The AI will:

1. Calculate timestamps (now - 24h to now)
2. Call `hyperliquid_get_user_fills` with time range
3. Format and display the results

## Asset Index Reference

Use `hyperliquid_get_meta` to get the full list. Common assets:

| Index | Asset | Index | Asset | Index | Asset |
| ----- | ----- | ----- | ----- | ----- | ----- |
| 0     | BTC   | 1     | ETH   | 5     | SOL   |
| 10    | LTC   | 11    | ARB   | 14    | SUI   |
| 18    | LINK  | 25    | XRP   | 27    | APT   |

## Order Types

### Limit Order (Good-Till-Cancel)

```python
order_type = {"limit": {"tif": "Gtc"}}
```

### Market Order (Immediate or Cancel)

```python
price = "0"  # Setting price to 0 makes it a market order
order_type = {"limit": {"tif": "Ioc"}}
```

### Trigger Order (Stop Loss / Take Profit)

```python
order_type = {
    "trigger": {
        "triggerPx": "100.5",  # Trigger price
        "isMarket": False,      # False for limit, True for market
        "tpsl": "tp"            # "tp" for take profit, "sl" for stop loss
    }
}
```

## Error Handling

### "User or API Wallet does not exist"

**Problem:** Your wallet isn't registered on Hyperliquid.

**Solution:**

1. Go to app.hyperliquid.xyz (or testnet URL)
2. Connect your wallet
3. Deposit any amount from Arbitrum
4. This registers your wallet

### "Order value must be at least $10"

**Problem:** Your order size is too small.

**Solution:** Ensure `size * price >= $10`

Example:

- SOL at $200: Need at least 0.05 SOL
- BTC at $50,000: Need at least 0.0002 BTC

### "Invalid signature"

**Problem:** Private key mismatch or signing error.

**Solution:**

1. Check your HYPERLIQUID_PRIVATE_KEY is correct
2. Ensure it matches the wallet address you registered
3. If using agent mode, verify HYPERLIQUID_ACCOUNT_ADDRESS

## Agent Mode (Advanced)

Agent mode allows an API wallet to sign transactions for a different trading account.

**Use case:** Keep your main account safe while allowing an API wallet to trade.

**Setup:**

```bash
HYPERLIQUID_PRIVATE_KEY=0xApiWalletPrivateKey...
HYPERLIQUID_ACCOUNT_ADDRESS=0xMainTradingAccountAddress...
```

**Requirements:**

1. Both wallets must be registered on Hyperliquid
2. Main account must approve the API wallet as an agent
3. Use `approve_agent` action through Hyperliquid UI first

## Security Best Practices

1. **Never commit private keys** - Always use environment variables
2. **Use testnet first** - Test strategies before going live
3. **Set up stop losses** - Use bracket orders for risk management
4. **Monitor positions** - Regularly check your account
5. **Use agent mode** - For production, keep main account key offline
6. **Start small** - Test with minimum order sizes first

## Troubleshooting

### Server won't start

```bash
# Check Python version
python --version  # Should be 3.10+

# Check dependencies
uv sync

# Check environment variables
cat .env

# Run with debug logging
HYPERLIQUID_LOG_LEVEL=DEBUG uvx --from mcp-hyperliquid hyperliquid-mcp
```

### Orders not placing

1. Check wallet is registered (see error handling)
2. Verify order size meets $10 minimum
3. Check you have sufficient balance
4. Ensure asset index is correct (use `get_meta`)

### Can't find asset

```
Use the hyperliquid_get_meta tool to get all asset indices
```

The AI will show you the complete list of tradeable assets with their indices.

## Development

### Local Development

```bash
# Clone the repository
git clone https://github.com/Dakkshin/hyperliquid-mcp.git
cd hyperliquid-mcp

# Install dependencies
uv sync

# Run locally
uv run python -m hyperliquid_mcp.server

# Run tests (when available)
uv run pytest
```

### Code Structure

```
hyperliquid-mcp/
├── src/
│   └── hyperliquid_mcp/
│       ├── __init__.py
│       └── server.py          # Main MCP server implementation
├── pyproject.toml             # Project configuration
├── README.md                  # This file
└── .env.example              # Environment template
```

### **Join Our Community**

- **[Telegram Group](https://t.me/+fC8GWO3zBe04NTY0)** - Get help, share strategies, and connect with other traders

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Resources

- [Hyperliquid Official Docs](https://hyperliquid.gitbook.io/)
- [Hyperliquid Python SDK](https://github.com/hyperliquid-dex/hyperliquid-python-sdk)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [MCP Servers Repository](https://github.com/modelcontextprotocol/servers)

## Support

- **Issues:** Open an issue on GitHub
- **Hyperliquid Support:** https://hyperliquid.gitbook.io/hyperliquid-docs/help/support
- **MCP Documentation:** https://modelcontextprotocol.io/

## Disclaimer

This software is provided "as is" without warranty. Trading cryptocurrencies carries significant risk. Only trade with funds you can afford to lose. The authors are not responsible for any trading losses.

---

**Happy Trading! 🚀**
