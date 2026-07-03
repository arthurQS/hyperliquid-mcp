"""Hyperliquid MCP Server - Main implementation."""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from typing import Any, Optional

import eth_account
import numpy as np
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.utils.types import Cloid
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Environment variables are now loaded from client config (MCP settings)
# No need for dotenv - variables come from the env section in mcp.json


class HyperliquidMCPServer:
    """MCP Server for Hyperliquid trading using the official Python SDK."""

    def __init__(self):
        """Initialize the Hyperliquid MCP server."""
        self.server = Server("hyperliquid-mcp")

        # Load configuration from environment
        self.private_key = os.getenv("HYPERLIQUID_PRIVATE_KEY")
        self.account_address = os.getenv("HYPERLIQUID_ACCOUNT_ADDRESS")
        self.vault_address = os.getenv("HYPERLIQUID_VAULT_ADDRESS")
        self.testnet = os.getenv("HYPERLIQUID_TESTNET", "").lower() == "true"

        if not self.private_key:
            raise ValueError("HYPERLIQUID_PRIVATE_KEY environment variable is required")

        # WebSocket state engine: in-memory mirror of the market fed by the
        # SDK's background WebSocket. Guarded by a lock because SDK callbacks
        # fire on the WS I/O thread, not the MCP event loop thread.
        self._state_lock = threading.Lock()
        self.local_book: dict[str, dict] = (
            {}
        )  # coin -> {"levels", "time", "received_at"}
        self._subscribed_books: set[str] = (
            set()
        )  # coins with a live l2Book subscription
        self.local_fills: deque = deque(maxlen=200)  # rolling recent live fills
        self._fills_subscribed = False
        self.local_trades: dict[str, deque] = (
            {}
        )  # coin -> deque(maxlen=1000) of executed trades
        self._subscribed_trades: set[str] = (
            set()
        )  # coins with a live trades subscription
        self._trades_last_recv: dict[str, float] = (
            {}
        )  # coin -> wall-clock of last WS trades msg

        # Initialize Hyperliquid SDK
        self._init_hyperliquid()

        # Register handlers
        self._register_handlers()

    def _init_hyperliquid(self):
        """Initialize Hyperliquid Exchange and Info instances."""
        try:
            # Create wallet from private key
            self.wallet: LocalAccount = eth_account.Account.from_key(self.private_key)

            # Determine account address (for agent mode support)
            if not self.account_address:
                self.account_address = self.wallet.address
                logger.info(f"Using wallet address as account: {self.account_address}")
            else:
                logger.info(
                    f"Agent mode: API wallet {self.wallet.address} signing for account {self.account_address}"
                )

            # Set base URL based on testnet flag
            base_url = (
                constants.TESTNET_API_URL if self.testnet else constants.MAINNET_API_URL
            )
            logger.info(f"Connecting to: {base_url}")

            # Discover all perp dexes (default "" plus any builder-deployed
            # ones, e.g. the HIP-3 dex hosting equity/commodity perps like
            # META, AAPL, gold) so their coin names resolve everywhere.
            bootstrap_info = Info(base_url, skip_ws=True)
            perp_dex_list = bootstrap_info.perp_dexs()
            dex_names = [""] + [d["name"] for d in perp_dex_list[1:] if d]
            logger.info(f"Discovered perp dexes: {dex_names}")

            # Initialize Info (read-only queries) with all perp dex universes
            # loaded. skip_ws=False starts the SDK's background WebSocket manager
            # (thread-based) so we can subscribe to live l2Book / userFills streams.
            self.info = Info(base_url, skip_ws=False, perp_dexs=dex_names)

            # Reverse map for asset-index -> coin-name resolution across all
            # dexes (default dex indices 0..N, builder dexes offset by
            # 110000 + i*10000 per the SDK's convention).
            self.asset_index_to_name = {
                v: k for k, v in self.info.coin_to_asset.items()
            }

            # Initialize Exchange (trading operations). perp_dexs must match
            # Info's, or the Exchange's internal name_to_coin map can't resolve
            # builder-dex coins (e.g. "xyz:META") and orders on them KeyError.
            self.exchange = Exchange(
                wallet=self.wallet,
                base_url=base_url,
                account_address=self.account_address,
                vault_address=self.vault_address,
                perp_dexs=dex_names,
            )

            # Verify wallet is registered
            try:
                user_state = self.info.user_state(self.account_address)
                logger.info(
                    f"✅ Wallet verified! Account value: ${user_state['marginSummary']['accountValue']}"
                )
            except Exception as e:
                logger.warning(f"⚠️  Could not verify wallet: {e}")
                logger.warning(
                    "Make sure your wallet is registered on Hyperliquid (deposit funds to register)"
                )

        except Exception as e:
            logger.error(f"Failed to initialize Hyperliquid SDK: {e}")
            raise

    # ------------------------------------------------------------------
    # WebSocket state engine
    #
    # The SDK's WebSocket is thread-based: these callbacks run on the WS I/O
    # thread, so they only touch shared state under self._state_lock and return
    # immediately (no network / no blocking) to avoid stalling the read loop.
    # Hyperliquid's l2Book pushes a full snapshot per message, so maintaining
    # the book is just replacing the coin's entry — no delta merge needed.
    # ------------------------------------------------------------------

    def _on_l2_book(self, msg: dict) -> None:
        """WS callback: mirror the latest order book snapshot for a coin."""
        data = msg.get("data", {})
        coin = data.get("coin")
        if not coin:
            return
        with self._state_lock:
            self.local_book[coin] = {
                "levels": data.get("levels"),
                "time": data.get("time"),
                "received_at": time.time(),
            }

    def _on_user_fills(self, msg: dict) -> None:
        """WS callback: append live fills (snapshot backfill + incremental)."""
        data = msg.get("data", {})
        fills = data.get("fills", []) or []
        with self._state_lock:
            self.local_fills.extend(fills)

    def _on_trades(self, coin: str, msg: dict) -> None:
        """WS callback: append the latest batch of executed trades for a coin.

        Unlike l2Book (a full-snapshot replace), the trades stream accumulates,
        so we extend the coin's deque. `coin` is captured via a per-subscription
        closure (see _ensure_trades_subscription) so the deque is keyed by the
        exact name the tool requested, sidestepping any WS name remapping.
        """
        data = msg.get("data") or []
        if not data:
            return
        with self._state_lock:
            dq = self.local_trades.get(coin)
            if dq is None:
                dq = deque(maxlen=1000)
                self.local_trades[coin] = dq
            dq.extend(data)
            self._trades_last_recv[coin] = time.time()

    def _ensure_book_subscription(self, coin: str) -> None:
        """Subscribe to l2Book for coin once; safe to call repeatedly."""
        with self._state_lock:
            if coin in self._subscribed_books:
                return
            self._subscribed_books.add(coin)
        try:
            # info.subscribe remaps the coin name via name_to_coin, so
            # dex-prefixed names (e.g. "xyz:META") pass through unchanged.
            self.info.subscribe({"type": "l2Book", "coin": coin}, self._on_l2_book)
        except Exception as e:
            logger.warning(f"Failed to subscribe l2Book for {coin}: {e}")
            with self._state_lock:
                self._subscribed_books.discard(coin)  # allow retry next call

    def _get_book(self, coin: str, stale_secs: float = 2.0) -> tuple[dict, str]:
        """Return (book, source) for coin.

        Serves the in-memory WS mirror when fresh (< stale_secs old), otherwise
        a REST snapshot. Lazily primes the l2Book subscription on first use.
        `book` has the SDK shape {"coin", "levels": [bids, asks], "time"}.
        """
        self._ensure_book_subscription(coin)
        with self._state_lock:
            cached = self.local_book.get(coin)
        if cached and (time.time() - cached["received_at"]) < stale_secs:
            return {
                "coin": coin,
                "levels": cached["levels"],
                "time": cached["time"],
            }, "websocket"
        return self.info.l2_snapshot(coin), "rest"

    def _ensure_trades_subscription(self, coin: str) -> None:
        """Subscribe to the market trades stream for coin once; safe to repeat.

        The callback is bound to the requested coin via a default-arg closure so
        the deque is keyed by that name regardless of how the SDK labels the
        inbound message.
        """
        with self._state_lock:
            if coin in self._subscribed_trades:
                return
            self._subscribed_trades.add(coin)
        try:
            self.info.subscribe(
                {"type": "trades", "coin": coin},
                lambda m, c=coin: self._on_trades(c, m),
            )
        except Exception as e:
            logger.warning(f"Failed to subscribe trades for {coin}: {e}")
            with self._state_lock:
                self._subscribed_trades.discard(coin)  # allow retry next call

    def _recent_trades_rest(self, coin: str, cutoff_ms: float) -> list:
        """REST pull of recent trades for coin, filtered to the time window.

        Raw info.post skips the SDK's name_to_coin remap that every proper Info
        method applies, so remap here or dex-prefixed names break.
        """
        api_coin = self.info.name_to_coin.get(coin, coin)
        result = (
            self.info.post("/info", {"type": "recentTrades", "coin": api_coin}) or []
        )
        return [t for t in result if t.get("time", 0) >= cutoff_ms]

    def _get_trades(
        self, coin: str, window_secs: int, cold_secs: float = 30.0
    ) -> tuple[list, str]:
        """Return (trades, source) — executed trades within the last window_secs.

        Serves the in-memory WS mirror when we have confirmed liveness, otherwise
        a REST snapshot. Lazily primes the trades subscription on first use.

        Trade-stream staleness is not the same as book staleness: a live market
        can be legitimately silent for a while, so silence alone must not force a
        REST round-trip. We fall back to REST only on cold start (no WS data yet,
        handles the subscribe->data race) or when the window is empty AND we
        cannot confirm the socket is alive (no message within cold_secs) — the
        mandatory guard against a silently-dead socket, since run_forever() does
        not auto-reconnect.
        """
        self._ensure_trades_subscription(coin)
        cutoff_ms = time.time() * 1000 - window_secs * 1000
        with self._state_lock:
            dq = self.local_trades.get(coin)
            last_recv = self._trades_last_recv.get(coin)
            snapshot = list(dq) if dq else []

        # Cold start: never received a WS message for this coin yet -> REST prime.
        if last_recv is None:
            return self._recent_trades_rest(coin, cutoff_ms), "rest"

        windowed = [t for t in snapshot if t.get("time", 0) >= cutoff_ms]
        if windowed:
            return windowed, "websocket"

        # Empty window: trust the silence if we heard from the socket recently
        # (a real "no flow"); otherwise fall back to REST as the dead-socket guard.
        if (time.time() - last_recv) < cold_secs:
            return [], "websocket"
        return self._recent_trades_rest(coin, cutoff_ms), "rest"

    @staticmethod
    def _orderflow(trades: list, window_secs: int) -> Optional[dict]:
        """Compute edge-level trade-flow features from executed trades.

        Returns None if there are no trades in the window (mirrors
        _microstructure on an empty book). Hyperliquid tags each trade with the
        aggressor side: "B" = buy aggressor, "A" = sell aggressor (verified
        against the SDK's ccxt parse_trade). TFI is the signed order-flow
        imbalance in [-1, 1] (0 = balanced); CVD is buy minus sell volume.
        """
        if not trades:
            return None

        buy_vol = 0.0
        sell_vol = 0.0
        notional = 0.0
        last_px = None
        min_t = None
        max_t = None
        for t in trades:
            sz = float(t["sz"])
            px = float(t["px"])
            if t.get("side") == "B":
                buy_vol += sz
            else:
                sell_vol += sz
            notional += px * sz
            last_px = px
            ts = t.get("time", 0)
            min_t = ts if min_t is None else min(min_t, ts)
            max_t = ts if max_t is None else max(max_t, ts)

        total = buy_vol + sell_vol
        cvd = buy_vol - sell_vol
        tfi = cvd / total if total else 0.0
        vwap = notional / total if total else last_px
        duration_s = (
            ((max_t - min_t) / 1000.0)
            if (min_t is not None and max_t is not None)
            else 0.0
        )

        return {
            "buy_vol": round(buy_vol, 6),
            "sell_vol": round(sell_vol, 6),
            "CVD": round(cvd, 6),
            "TFI": round(tfi, 4),
            "trades": len(trades),
            "vwap": round(vwap, 8) if vwap is not None else None,
            "last_px": round(last_px, 8) if last_px is not None else None,
            "duration_s": round(duration_s, 1),
        }

    @staticmethod
    def _microstructure(levels: list, depth: int) -> Optional[dict]:
        """Compute edge-level microstructure features from raw book levels.

        Returns None if either side is empty. `levels` is [bids, asks] where
        each level is {"px": str, "sz": str, "n": int}; bids are best-first
        (descending), asks best-first (ascending).
        """
        bids = (levels[0] or [])[:depth]
        asks = (levels[1] or [])[:depth]
        if not bids or not asks:
            return None

        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        best_bid_sz = float(bids[0]["sz"])
        best_ask_sz = float(asks[0]["sz"])

        bid_vol = sum(float(l["sz"]) for l in bids)
        ask_vol = sum(float(l["sz"]) for l in asks)

        # Order Book Imbalance over the top `depth` levels: bid share of total
        # displayed volume. >0.5 => bids heavier (upward pressure).
        total_vol = bid_vol + ask_vol
        obi = bid_vol / total_vol if total_vol else 0.5

        # Stoikov micro-price: each side's price weighted by the OPPOSITE side's
        # top size, so heavy asks pull the fair price toward the bid. Reacts
        # faster than the plain mid.
        tob_sz = best_bid_sz + best_ask_sz
        mid = (best_bid + best_ask) / 2
        micro = (
            ((best_bid * best_ask_sz + best_ask * best_bid_sz) / tob_sz)
            if tob_sz
            else mid
        )

        # Spread in basis points relative to the mid.
        spread_bps = ((best_ask - best_bid) / mid * 10000) if mid else 0.0

        return {
            "OBI": round(obi, 4),
            "micro_price": round(micro, 8),
            "mid": round(mid, 8),
            "spread_bps": round(spread_bps, 3),
        }

    @staticmethod
    def _monte_carlo(
        closes: list,
        s0: float,
        steps: int,
        iterations: int,
        use_historical_drift: bool,
        rng: Optional[np.random.Generator] = None,
    ) -> Optional[dict]:
        """Vectorized GBM Monte Carlo risk engine.

        `closes` is a chronological close-price series (strings or floats).
        Estimates per-step volatility (sigma) — and, if requested, the per-step
        mean log return (m) — from log returns, then samples terminal prices.

        Only terminal statistics are reported, and a sum of `steps` iid normal
        log increments is itself normal, so log(S_T) is sampled directly:

            log S_T = log s0 + steps*m + sigma*sqrt(steps)*Z,  Z ~ N(0,1)

        One draw per path — O(iterations) time/memory regardless of horizon,
        with a distribution identical to simulating full paths step by step.

        Drift semantics: the observed mean log return already embeds GBM's
        Ito correction (E[log ret] = mu - sigma^2/2), so with historical drift
        it is used as-is — no extra -sigma^2/2. "Zero drift" means a martingale
        price (E[S_T] = s0, zero expected *return*), i.e. m = -sigma^2/2.

        Returns an aggregated risk profile (terminal-price distribution, 5% VaR,
        prob-of-profit). Returns None on bad input: fewer than 3 closes,
        non-positive prices (log undefined), or zero/non-finite volatility.
        """
        arr = np.asarray([float(c) for c in closes], dtype=float)
        if arr.size < 3 or s0 <= 0 or (arr <= 0).any():
            return None

        logret = np.diff(np.log(arr))
        sigma = float(logret.std(ddof=1))
        if sigma == 0.0 or not np.isfinite(sigma):
            return None
        m = float(logret.mean()) if use_historical_drift else -0.5 * sigma**2

        rng = rng if rng is not None else np.random.default_rng()
        z = rng.standard_normal(iterations)
        terminal = s0 * np.exp(steps * m + sigma * np.sqrt(steps) * z)

        ret = terminal / s0 - 1.0  # terminal returns
        var5_ret = float(np.percentile(ret, 5))  # 5% quantile of returns
        return {
            "s0": round(s0, 8),
            "steps": steps,
            "iterations": iterations,
            "sigma_per_step": round(sigma, 8),
            "mu_per_step": round(m, 8),
            "mean_terminal": round(float(terminal.mean()), 8),
            "median_terminal": round(float(np.percentile(terminal, 50)), 8),
            "p05_terminal": round(float(np.percentile(terminal, 5)), 8),
            "p95_terminal": round(float(np.percentile(terminal, 95)), 8),
            "expected_return": round(float(ret.mean()), 6),
            "VaR_5pct": round(-var5_ret, 6),  # positive = loss magnitude at 5%
            "prob_profit": round(float((terminal > s0).mean()), 4),
        }

    def _start_streams(self) -> None:
        """Subscribe account-wide userFills once (called at server startup)."""
        if self._fills_subscribed:
            return
        try:
            self.info.subscribe(
                {"type": "userFills", "user": self.account_address},
                self._on_user_fills,
            )
            self._fills_subscribed = True
        except Exception as e:
            logger.warning(f"Failed to subscribe userFills: {e}")

    def _register_handlers(self):
        """Register all MCP handlers."""

        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """List all available Hyperliquid tools."""
            return [
                # Account & Position Management
                Tool(
                    name="hyperliquid_get_account_info",
                    description="Get user's perpetual account summary including positions and margin",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional, defaults to empty string)",
                                "default": "",
                            },
                        },
                    },
                ),
                Tool(
                    name="hyperliquid_get_positions",
                    description="Get user's open positions with margin summary",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional, defaults to empty string)",
                                "default": "",
                            },
                        },
                    },
                ),
                Tool(
                    name="hyperliquid_get_balance",
                    description="Get user's account balance and withdrawable amount",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional, defaults to empty string)",
                                "default": "",
                            },
                        },
                    },
                ),
                # Order Management
                Tool(
                    name="hyperliquid_place_order",
                    description="Place a single order on Hyperliquid. Minimum order value is $10. Use asset index from get_meta (e.g., 0=BTC, 1=ETH, 5=SOL).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "asset": {
                                "type": "integer",
                                "description": "Asset index (e.g., 0 for BTC, 1 for ETH, 5 for SOL). Use hyperliquid_get_meta to get the full list.",
                                "minimum": 0,
                            },
                            "isBuy": {
                                "type": "boolean",
                                "description": "True for buy/long orders, false for sell/short orders",
                            },
                            "size": {
                                "type": "string",
                                "description": "Order size/quantity as a string (e.g., '0.1' for 0.1 BTC). Ensure size * price >= $10.",
                            },
                            "price": {
                                "type": "string",
                                "description": "Limit price as a string (e.g., '181.5'). Set to '0' for a market-style order: executed as an aggressive IoC limit at mid +/- 5% slippage protection.",
                            },
                            "reduceOnly": {
                                "type": "boolean",
                                "description": "Whether this is a reduce-only order (only closes existing positions)",
                                "default": False,
                            },
                            "orderType": {
                                "type": "object",
                                "description": "Order type configuration. For limit orders use {limit: {tif: 'Gtc'}}. For trigger orders use {trigger: {isMarket: false, triggerPx: 'price', tpsl: 'tp' or 'sl'}}",
                                "default": {"limit": {"tif": "Gtc"}},
                            },
                            "cloid": {
                                "type": "string",
                                "description": "Client order ID (optional, for tracking)",
                            },
                        },
                        "required": ["asset", "isBuy", "size"],
                    },
                ),
                Tool(
                    name="hyperliquid_place_bracket_order",
                    description="Place a complete bracket order (entry + take profit + stop loss) in a single atomic batch. Minimum order value is $10. The TP and SL are reduce-only trigger orders; the TP rests as a limit at its price, the SL triggers as a market order (with 5% slippage bound) so it cannot gap through unfilled.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "asset": {
                                "type": "integer",
                                "description": "Asset index (e.g., 0 for BTC, 1 for ETH, 5 for SOL)",
                                "minimum": 0,
                            },
                            "isBuy": {
                                "type": "boolean",
                                "description": "True for buy/long positions, false for sell/short positions",
                            },
                            "size": {
                                "type": "string",
                                "description": "Position size as a string (e.g., '4.96' for 4.96 SOL)",
                            },
                            "entryPrice": {
                                "type": "string",
                                "description": "Entry limit price as a string (e.g., '181.5'). Set to '0' for market-style entry: executed as an aggressive IoC limit at mid +/- 5% slippage protection.",
                            },
                            "takeProfitPrice": {
                                "type": "string",
                                "description": "Take profit trigger price. For long: above entry. For short: below entry.",
                            },
                            "stopLossPrice": {
                                "type": "string",
                                "description": "Stop loss trigger price. For long: below entry. For short: above entry.",
                            },
                            "reduceOnly": {
                                "type": "boolean",
                                "description": "Whether the ENTRY order is reduce-only (usually false)",
                                "default": False,
                            },
                            "entryOrderType": {
                                "type": "object",
                                "description": "Entry order type configuration",
                                "default": {"limit": {"tif": "Gtc"}},
                            },
                        },
                        "required": [
                            "asset",
                            "isBuy",
                            "size",
                            "takeProfitPrice",
                            "stopLossPrice",
                        ],
                    },
                ),
                Tool(
                    name="hyperliquid_cancel_order",
                    description="Cancel a specific order by coin name and order ID (oid). Always use oid for cancellation.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Coin/asset name (e.g., 'BTC', 'ETH', 'SOL')",
                            },
                            "oid": {
                                "type": "integer",
                                "description": "Order ID (oid) - the unique order identifier returned when order was placed",
                            },
                        },
                        "required": ["coin", "oid"],
                    },
                ),
                Tool(
                    name="hyperliquid_cancel_all_orders",
                    description="Cancel all open orders for the user. Fetches all open orders and cancels them.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional)",
                                "default": "",
                            },
                        },
                    },
                ),
                Tool(
                    name="hyperliquid_modify_order",
                    description="Modify an existing order",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "oid": {
                                "type": "integer",
                                "description": "Order ID to modify",
                            },
                            "coin": {
                                "type": "string",
                                "description": "Coin/asset name (e.g., 'BTC', 'ETH', 'SOL')",
                            },
                            "isBuy": {
                                "type": "boolean",
                                "description": "True for buy orders, false for sell orders",
                            },
                            "size": {"type": "string", "description": "New order size"},
                            "price": {
                                "type": "string",
                                "description": "New limit price",
                            },
                            "reduceOnly": {
                                "type": "boolean",
                                "description": "Whether this is a reduce-only order",
                                "default": False,
                            },
                            "orderType": {
                                "type": "object",
                                "description": "Order type configuration",
                                "default": {"limit": {"tif": "Gtc"}},
                            },
                        },
                        "required": ["oid", "coin", "isBuy", "size", "price"],
                    },
                ),
                Tool(
                    name="hyperliquid_place_twap_order",
                    description="Place a Time-Weighted Average Price (TWAP) order",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Coin/asset name (e.g., 'BTC', 'ETH', 'SOL')",
                            },
                            "isBuy": {
                                "type": "boolean",
                                "description": "True for buy orders, false for sell orders",
                            },
                            "size": {
                                "type": "string",
                                "description": "Total order size to be executed over time",
                            },
                            "minutes": {
                                "type": "integer",
                                "description": "Duration in minutes for TWAP execution",
                                "minimum": 2,
                            },
                            "reduceOnly": {
                                "type": "boolean",
                                "description": "Whether this is a reduce-only order",
                                "default": False,
                            },
                            "randomize": {
                                "type": "boolean",
                                "description": "Whether to randomize TWAP intervals",
                                "default": True,
                            },
                        },
                        "required": ["coin", "isBuy", "size", "minutes"],
                    },
                ),
                Tool(
                    name="hyperliquid_cancel_twap_order",
                    description="Cancel a TWAP order by its ID",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "twapId": {
                                "type": "integer",
                                "description": "TWAP order ID to cancel",
                            }
                        },
                        "required": ["twapId"],
                    },
                ),
                # Order Queries
                Tool(
                    name="hyperliquid_get_open_orders",
                    description="Get user's currently open orders",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional)",
                                "default": "",
                            },
                        },
                    },
                ),
                Tool(
                    name="hyperliquid_get_order_status",
                    description="Get the status of a specific order by oid",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "oid": {"type": "integer", "description": "Order ID"},
                        },
                        "required": ["oid"],
                    },
                ),
                Tool(
                    name="hyperliquid_get_user_fills",
                    description="Get user's historical trade fills",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "startTime": {
                                "type": "integer",
                                "description": "Start time in milliseconds (required)",
                            },
                            "endTime": {
                                "type": "integer",
                                "description": "End time in milliseconds (optional, defaults to current time)",
                            },
                            "aggregateByTime": {
                                "type": "boolean",
                                "description": "Whether to aggregate partial fills by time",
                                "default": False,
                            },
                        },
                        "required": ["startTime"],
                    },
                ),
                Tool(
                    name="hyperliquid_get_user_funding",
                    description="Get user's funding payment history",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "userAddress": {
                                "type": "string",
                                "description": "User address (optional, defaults to configured account)",
                            },
                            "startTime": {
                                "type": "integer",
                                "description": "Start time in milliseconds (required)",
                            },
                            "endTime": {
                                "type": "integer",
                                "description": "End time in milliseconds (optional, defaults to current time)",
                            },
                        },
                        "required": ["startTime"],
                    },
                ),
                # Market Data
                Tool(
                    name="hyperliquid_get_meta",
                    description="Get exchange metadata including all available trading assets with their indices, names, max leverage, and trading parameters. Essential for mapping coin names to asset indices. Defaults to the main perp dex; pass 'dex' to inspect a builder-deployed dex (e.g. the one hosting equity/commodity perps like META, AAPL). Use hyperliquid_get_perp_dexs to discover dex names. Note: the 'index' values returned are relative to that dex's own universe - for hyperliquid_place_order's global asset index, add 110000 + i*10000 (i = the dex's 0-based position among builder dexes, excluding the default dex) for non-default dexes.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional, defaults to the main dex)",
                                "default": "",
                            }
                        },
                    },
                ),
                Tool(
                    name="hyperliquid_get_perp_dexs",
                    description="List all available perp dexes, including builder-deployed ones (e.g. the dex hosting equity/commodity perps like META, AAPL, gold). Use the returned dex names with hyperliquid_get_meta/hyperliquid_get_all_mids to inspect a specific dex.",
                    inputSchema={"type": "object", "properties": {}},
                ),
                Tool(
                    name="hyperliquid_get_all_mids",
                    description="Get current mid prices for all assets. Defaults to the main perp dex; pass 'dex' to get mids for a builder-deployed dex.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional, defaults to the main dex)",
                                "default": "",
                            }
                        },
                    },
                ),
                Tool(
                    name="hyperliquid_get_order_book",
                    description="Get order book (market depth) for a specific asset. Served instantly from a live in-memory WebSocket mirror when fresh (falls back to REST). Returns the top `depth` levels per side.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'SOL'). For builder-deployed dex assets like equities/commodities, use the dex-prefixed name (e.g. 'xyz:META' for Meta Platforms) - see hyperliquid_get_perp_dexs and hyperliquid_get_meta.",
                            },
                            "depth": {
                                "type": "integer",
                                "description": "Number of price levels to return per side (default 5). The immediate spread carries most of the actionable signal; keep this small to save tokens. Use a larger value only when you need deeper liquidity context.",
                            },
                        },
                        "required": ["coin"],
                    },
                ),
                Tool(
                    name="hyperliquid_get_microstructure",
                    description="Get dense edge-level microstructure signals for an asset, computed server-side from the live order book: Order Book Imbalance (OBI, bid share of top-depth volume, >0.5 = upward pressure), Stoikov micro-price (fair value that reacts faster than the mid), mid price, and bid-ask spread in basis points. Returns a tiny flat payload (~20 tokens) instead of raw book JSON - prefer this over hyperliquid_get_order_book when you only need to gauge market pressure / execution conditions rather than inspect individual levels.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'HYPE'). For builder-deployed dex assets, use the dex-prefixed name (e.g. 'xyz:META').",
                            },
                            "depth": {
                                "type": "integer",
                                "description": "Number of levels per side to include in the OBI calculation (default 5). Micro-price and spread use top-of-book.",
                            },
                        },
                        "required": ["coin"],
                    },
                ),
                Tool(
                    name="hyperliquid_get_orderflow",
                    description="Get dense edge-level trade-flow signals for an asset, computed server-side from the live trade tape over a recent window: aggressor buy vs sell volume, CVD (cumulative volume delta = buy_vol - sell_vol), TFI (trade-flow imbalance in [-1,1], >0 = net buying pressure), trade count, window VWAP, last price, and window duration. This is the trade-flow analog of OBI (which measures the resting book) - use it to gauge whether executed flow is buyer- or seller-driven. Returns a tiny flat payload (~20 tokens) instead of raw trade JSON - prefer this over hyperliquid_get_recent_trades when you only need aggregate aggressor pressure rather than individual prints.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'HYPE'). For builder-deployed dex assets, use the dex-prefixed name (e.g. 'xyz:META').",
                            },
                            "window_secs": {
                                "type": "integer",
                                "description": "Lookback window in seconds over recent executed trades (default 60).",
                            },
                        },
                        "required": ["coin"],
                    },
                ),
                Tool(
                    name="hyperliquid_get_recent_trades",
                    description="Get recent trades for a specific asset",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'SOL'). For builder-deployed dex assets like equities/commodities, use the dex-prefixed name (e.g. 'xyz:META' for Meta Platforms) - see hyperliquid_get_perp_dexs and hyperliquid_get_meta.",
                            }
                        },
                        "required": ["coin"],
                    },
                ),
                Tool(
                    name="hyperliquid_get_historical_funding",
                    description="Get historical funding rates for an asset",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'SOL'). For builder-deployed dex assets like equities/commodities, use the dex-prefixed name (e.g. 'xyz:META' for Meta Platforms) - see hyperliquid_get_perp_dexs and hyperliquid_get_meta.",
                            },
                            "startTime": {
                                "type": "integer",
                                "description": "Start time in milliseconds",
                            },
                            "endTime": {
                                "type": "integer",
                                "description": "End time in milliseconds (optional, defaults to current time)",
                            },
                        },
                        "required": ["coin", "startTime"],
                    },
                ),
                Tool(
                    name="hyperliquid_get_candles",
                    description="Get historical candle/OHLCV data for an asset",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'SOL'). For builder-deployed dex assets like equities/commodities, use the dex-prefixed name (e.g. 'xyz:META' for Meta Platforms) - see hyperliquid_get_perp_dexs and hyperliquid_get_meta.",
                            },
                            "interval": {
                                "type": "string",
                                "description": "Candle interval",
                                "enum": ["1m", "5m", "15m", "1h", "4h", "1d"],
                            },
                            "startTime": {
                                "type": "integer",
                                "description": "Start time in milliseconds",
                            },
                            "endTime": {
                                "type": "integer",
                                "description": "End time in milliseconds (optional, defaults to current time)",
                            },
                        },
                        "required": ["coin", "interval", "startTime"],
                    },
                ),
                Tool(
                    name="hyperliquid_run_monte_carlo",
                    description="Run a vectorized Geometric Brownian Motion (GBM) Monte Carlo price simulation and return an aggregated risk profile (terminal-price distribution, 5% Value-at-Risk, probability of profit). Volatility (and optionally drift) is estimated from recent candles; thousands of paths are simulated server-side and only the summary is returned.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'SOL'). For builder-deployed dex assets like equities/commodities, use the dex-prefixed name (e.g. 'xyz:META' for Meta Platforms) - see hyperliquid_get_perp_dexs and hyperliquid_get_meta.",
                            },
                            "days_forward": {
                                "type": "integer",
                                "description": "Simulation horizon in days (optional, default 7)",
                                "default": 7,
                            },
                            "iterations": {
                                "type": "integer",
                                "description": "Number of Monte Carlo paths (optional, default 10000)",
                                "default": 10000,
                            },
                            "interval": {
                                "type": "string",
                                "description": "Candle interval used to estimate volatility and to step the simulation (optional, default '1h')",
                                "enum": ["1m", "5m", "15m", "1h", "4h", "1d"],
                                "default": "1h",
                            },
                            "lookback_days": {
                                "type": "integer",
                                "description": "How many days of history to estimate mu/sigma from (optional, default 30)",
                                "default": 30,
                            },
                            "use_historical_drift": {
                                "type": "boolean",
                                "description": "If true, use the historical mean log-return as drift (mu); if false (default), assume zero drift - more conservative for risk/VaR.",
                                "default": False,
                            },
                        },
                        "required": ["coin"],
                    },
                ),
                # Vault Management
                Tool(
                    name="hyperliquid_vault_details",
                    description="Get detailed information about a specific vault",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "vaultAddress": {
                                "type": "string",
                                "description": "Vault address in 42-character hexadecimal format",
                            }
                        },
                        "required": ["vaultAddress"],
                    },
                ),
                Tool(
                    name="hyperliquid_vault_performance",
                    description="Get performance metrics for a specific vault. Returns the vault's full portfolio history across the API's standard windows (day/week/month/allTime); no time-range filtering is available.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "vaultAddress": {
                                "type": "string",
                                "description": "Vault address in 42-character hexadecimal format",
                            },
                        },
                        "required": ["vaultAddress"],
                    },
                ),
                # Utility
                Tool(
                    name="hyperliquid_get_server_time",
                    description="Get estimated server time",
                    inputSchema={"type": "object", "properties": {}},
                ),
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict) -> list[TextContent]:
            """Handle tool calls."""
            try:
                # _handle_tool_call is synchronous (blocking requests-based SDK
                # calls, zero awaits); run it off-loop so a slow REST round-trip
                # can't stall the MCP event loop.
                result = await asyncio.to_thread(
                    self._handle_tool_call, name, arguments
                )
                # Compact separators (no indentation) ~halve the token cost of
                # every response with zero information loss.
                return [
                    TextContent(
                        type="text", text=json.dumps(result, separators=(",", ":"))
                    )
                ]
            except Exception as e:
                logger.error(f"Tool {name} failed: {e}", exc_info=True)
                return [
                    TextContent(
                        type="text",
                        text=json.dumps(
                            {"error": str(e), "tool": name, "arguments": arguments},
                            separators=(",", ":"),
                        ),
                    )
                ]

    def _handle_tool_call(self, name: str, arguments: dict) -> dict:
        """Route tool calls to appropriate handlers.

        Synchronous by design — every SDK call here is blocking; call_tool
        dispatches this on a worker thread via asyncio.to_thread.
        """

        # Normalize integer parameters (convert float to int if needed)
        integer_params = [
            "asset",
            "oid",
            "startTime",
            "endTime",
            "twapId",
            "minutes",
            "depth",
            "window_secs",
            "days_forward",
            "iterations",
            "lookback_days",
        ]
        for param in integer_params:
            if param in arguments and arguments[param] is not None:
                try:
                    arguments[param] = int(float(arguments[param]))
                    logger.debug(
                        f"Normalized {param} parameter: {arguments[param]} (type: {type(arguments[param])})"
                    )
                except (ValueError, TypeError) as e:
                    logger.error(
                        f"Failed to convert {param} parameter to integer: {arguments.get(param)} - {e}"
                    )
                    raise ValueError(
                        f"Invalid {param} parameter: {arguments.get(param)}. Must be a valid integer."
                    )

        # Get user address (use configured account if not provided; `or` also
        # covers clients that send an explicit null)
        user_address = arguments.get("userAddress") or self.account_address

        # Account & Position Management
        if name == "hyperliquid_get_account_info":
            dex = arguments.get("dex", "")
            result = self.info.user_state(user_address, dex=dex)
            return {
                "message": "Account information retrieved successfully",
                "data": result,
                "summary": {
                    "accountValue": result["marginSummary"]["accountValue"],
                    "totalMarginUsed": result["marginSummary"]["totalMarginUsed"],
                    "withdrawable": result["withdrawable"],
                    "numberOfPositions": len(result["assetPositions"]),
                },
            }

        elif name == "hyperliquid_get_positions":
            dex = arguments.get("dex", "")
            result = self.info.user_state(user_address, dex=dex)
            return {
                "message": "Positions retrieved successfully",
                "data": {
                    "assetPositions": result["assetPositions"],
                    "marginSummary": result["marginSummary"],
                    "crossMarginSummary": result.get("crossMarginSummary"),
                    "withdrawable": result["withdrawable"],
                },
                "summary": {
                    "numberOfPositions": len(result["assetPositions"]),
                    "accountValue": result["marginSummary"]["accountValue"],
                    "totalMarginUsed": result["marginSummary"]["totalMarginUsed"],
                },
            }

        elif name == "hyperliquid_get_balance":
            dex = arguments.get("dex", "")
            result = self.info.user_state(user_address, dex=dex)
            margin_summary = result["marginSummary"]
            return {
                "message": "Balance retrieved successfully",
                "data": {
                    "accountValue": margin_summary["accountValue"],
                    "totalMarginUsed": margin_summary["totalMarginUsed"],
                    "totalNtlPos": margin_summary["totalNtlPos"],
                    "totalRawUsd": margin_summary["totalRawUsd"],
                    "withdrawable": result["withdrawable"],
                },
                "summary": {
                    "accountValue": margin_summary["accountValue"],
                    "withdrawable": result["withdrawable"],
                    "availableBalance": str(
                        float(margin_summary["accountValue"])
                        - float(margin_summary["totalMarginUsed"])
                    ),
                },
            }

        # Order Management
        elif name == "hyperliquid_place_order":
            asset = arguments["asset"]  # Already normalized to integer
            is_buy = arguments["isBuy"]
            size = float(arguments["size"])
            # Keep price as string if provided, convert to float for SDK
            price_str = arguments.get("price", "0")
            price = float(price_str) if price_str else 0.0
            reduce_only = arguments.get("reduceOnly", False)
            order_type = arguments.get("orderType", {"limit": {"tif": "Gtc"}})
            cloid_str = arguments.get("cloid")

            # Convert asset index to coin name
            coin_name = self.asset_index_to_name.get(asset)
            if coin_name is None:
                raise ValueError(f"Unknown asset index: {asset}")

            # Create cloid if provided
            cloid = Cloid(cloid_str) if cloid_str else None

            # Handle trigger orders: convert triggerPx string to float
            if "trigger" in order_type:
                trigger = order_type["trigger"]
                if "triggerPx" in trigger and isinstance(trigger["triggerPx"], str):
                    trigger["triggerPx"] = float(trigger["triggerPx"])
            elif price == 0.0:
                # Hyperliquid has no native market orders: a resting buy limit
                # at 0 would never fill. Emulate market like the SDK's
                # market_open: aggressive IoC limit at mid +/- 5% slippage.
                price = self.exchange._slippage_price(
                    coin_name, is_buy, Exchange.DEFAULT_SLIPPAGE
                )
                order_type = {"limit": {"tif": "Ioc"}}

            result = self.exchange.order(
                name=coin_name,
                is_buy=is_buy,
                sz=size,
                limit_px=price,
                order_type=order_type,
                reduce_only=reduce_only,
                cloid=cloid,
            )

            # Parse response
            order_info = self._parse_order_response(result)

            return {
                "message": f"Order placed for {coin_name}",
                "data": result,
                "orderInfo": order_info,
                "requestParams": arguments,
            }

        elif name == "hyperliquid_place_bracket_order":
            asset = arguments["asset"]  # Already normalized to integer
            is_buy = arguments["isBuy"]
            size = float(arguments["size"])
            entry_price = float(arguments.get("entryPrice", 0))
            tp_price = float(arguments["takeProfitPrice"])
            sl_price = float(arguments["stopLossPrice"])
            reduce_only = arguments.get("reduceOnly", False)
            entry_order_type = arguments.get(
                "entryOrderType", {"limit": {"tif": "Gtc"}}
            )

            # Convert asset index to coin name
            coin_name = self.asset_index_to_name.get(asset)
            if coin_name is None:
                raise ValueError(f"Unknown asset index: {asset}")

            # Market entry (entryPrice 0/omitted): emulate with an aggressive
            # IoC limit at mid +/- 5% slippage — a real limit at 0 would rest
            # forever on the buy side.
            if entry_price == 0.0:
                entry_price = self.exchange._slippage_price(
                    coin_name, is_buy, Exchange.DEFAULT_SLIPPAGE
                )
                entry_order_type = {"limit": {"tif": "Ioc"}}

            # The stop-loss triggers as MARKET (isMarket True): a limit SL can
            # gap through its price and never fill, defeating the stop. Its
            # limit_px is the slippage-bounded worst fill around the trigger.
            sl_limit_px = self.exchange._slippage_price(
                coin_name, not is_buy, Exchange.DEFAULT_SLIPPAGE, px=sl_price
            )

            # Create order requests for bracket
            # Note: The SDK's exchange.order() expects floats, not strings
            # The SDK will handle the conversion to wire format internally
            orders = [
                # Entry order
                {
                    "coin": coin_name,
                    "is_buy": is_buy,
                    "sz": size,
                    "limit_px": entry_price,
                    "order_type": entry_order_type,
                    "reduce_only": reduce_only,
                },
                # Take profit order (opposite side, reduce-only)
                {
                    "coin": coin_name,
                    "is_buy": not is_buy,
                    "sz": size,
                    "limit_px": tp_price,
                    "order_type": {
                        "trigger": {
                            "triggerPx": tp_price,
                            "isMarket": False,
                            "tpsl": "tp",
                        }
                    },
                    "reduce_only": True,
                },
                # Stop loss order (opposite side, reduce-only, market trigger)
                {
                    "coin": coin_name,
                    "is_buy": not is_buy,
                    "sz": size,
                    "limit_px": sl_limit_px,
                    "order_type": {
                        "trigger": {
                            "triggerPx": sl_price,
                            "isMarket": True,
                            "tpsl": "sl",
                        }
                    },
                    "reduce_only": True,
                },
            ]

            result = self.exchange.bulk_orders(orders)

            # Parse response for all three orders
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            order_infos = []
            for idx, status in enumerate(statuses):
                order_type = ["entry", "take-profit", "stop-loss"][idx]
                info = self._parse_order_status(status)
                info["orderType"] = order_type
                order_infos.append(info)

            return {
                "message": "Bracket order placed successfully",
                "data": result,
                "orders": order_infos,
                "requestParams": arguments,
            }

        elif name == "hyperliquid_cancel_order":
            coin = arguments["coin"]
            oid = arguments["oid"]  # Already normalized to integer

            result = self.exchange.cancel(coin, oid)

            return {
                "message": f"Order {oid} cancelled for {coin}",
                "data": result,
                "cancelledOrder": {"coin": coin, "orderId": oid},
            }

        elif name == "hyperliquid_cancel_all_orders":
            dex = arguments.get("dex", "")

            # Get all open orders
            open_orders = self.info.open_orders(user_address, dex=dex)

            if not open_orders:
                return {
                    "message": "No open orders to cancel",
                    "data": {"status": "ok", "response": {"data": {"statuses": []}}},
                    "cancelledCount": 0,
                }

            # Build cancel requests
            cancel_requests = [
                {"coin": order["coin"], "oid": order["oid"]} for order in open_orders
            ]

            result = self.exchange.bulk_cancel(cancel_requests)

            return {
                "message": f"Cancelled {len(cancel_requests)} orders",
                "data": result,
                "cancelledCount": len(cancel_requests),
            }

        elif name == "hyperliquid_modify_order":
            oid = arguments["oid"]  # Already normalized to integer
            coin = arguments["coin"]
            is_buy = arguments["isBuy"]
            size = float(arguments["size"])
            price = float(arguments["price"])
            reduce_only = arguments.get("reduceOnly", False)
            order_type = arguments.get("orderType", {"limit": {"tif": "Gtc"}})

            result = self.exchange.modify_order(
                oid=oid,
                name=coin,
                is_buy=is_buy,
                sz=size,
                limit_px=price,
                order_type=order_type,
                reduce_only=reduce_only,
            )

            return {
                "message": f"Order {oid} modified successfully",
                "data": result,
                "modifiedOrder": {
                    "orderId": oid,
                    "coin": coin,
                    "newPrice": price,
                    "newSize": size,
                },
            }

        elif name == "hyperliquid_place_twap_order":
            # Note: TWAP requires special handling, not directly supported in basic SDK
            # This would need the TWAP action structure
            raise NotImplementedError("TWAP orders require additional implementation")

        elif name == "hyperliquid_cancel_twap_order":
            raise NotImplementedError(
                "TWAP cancellation requires additional implementation"
            )

        # Order Queries
        elif name == "hyperliquid_get_open_orders":
            dex = arguments.get("dex", "")
            result = self.info.open_orders(user_address, dex=dex)

            return {
                "message": "Open orders retrieved successfully",
                "data": result,
                "summary": {"numberOfOrders": len(result) if result else 0},
            }

        elif name == "hyperliquid_get_order_status":
            oid = arguments["oid"]  # Already normalized to integer
            result = self.info.query_order_by_oid(user_address, oid)

            return {
                "message": "Order status retrieved successfully",
                "data": result,
                "orderId": oid,
            }

        elif name == "hyperliquid_get_user_fills":
            start_time = arguments["startTime"]  # Already normalized to integer
            end_time = arguments.get(
                "endTime"
            )  # Already normalized to integer if present
            aggregate = arguments.get("aggregateByTime", False)

            result = self.info.user_fills_by_time(
                address=user_address,
                start_time=start_time,
                end_time=end_time,
                aggregate_by_time=aggregate,
            )

            return {
                "message": "User fills retrieved successfully",
                "data": result,
                "summary": {
                    "numberOfFills": len(result) if result else 0,
                    "timeRange": {
                        "startTime": start_time,
                        "endTime": end_time or "current",
                    },
                },
            }

        elif name == "hyperliquid_get_user_funding":
            start_time = arguments["startTime"]  # Already normalized to integer
            end_time = arguments.get(
                "endTime"
            )  # Already normalized to integer if present

            result = self.info.user_funding_history(
                user=user_address, startTime=start_time, endTime=end_time
            )

            return {
                "message": "User funding retrieved successfully",
                "data": result,
                "summary": {
                    "numberOfEntries": len(result) if result else 0,
                    "timeRange": {
                        "startTime": start_time,
                        "endTime": end_time or "current",
                    },
                },
            }

        # Market Data
        elif name == "hyperliquid_get_meta":
            dex = arguments.get("dex", "")
            result = self.info.meta(dex=dex)

            # Format universe with indices
            assets_with_indices = [
                {
                    "index": idx,
                    "name": asset["name"],
                    "maxLeverage": asset["maxLeverage"],
                    "onlyIsolated": asset.get("onlyIsolated", False),
                }
                for idx, asset in enumerate(result["universe"])
            ]

            return {
                "message": "Exchange metadata retrieved successfully",
                "data": result,
                "summary": {
                    "dex": dex,
                    "numberOfAssets": len(result["universe"]),
                    "assetsWithIndices": assets_with_indices,
                },
            }

        elif name == "hyperliquid_get_perp_dexs":
            result = self.info.perp_dexs()

            return {
                "message": "Perp dexes retrieved successfully",
                "data": result,
                "summary": {"numberOfDexes": len(result)},
            }

        elif name == "hyperliquid_get_all_mids":
            dex = arguments.get("dex", "")
            result = self.info.all_mids(dex=dex)

            return {
                "message": "All mid prices retrieved successfully",
                "data": result,
                "summary": {"dex": dex, "numberOfAssets": len(result)},
            }

        elif name == "hyperliquid_get_order_book":
            coin = arguments["coin"]
            depth = arguments.get("depth", 5)

            # Serve from the WS mirror when fresh, else REST (see _get_book).
            result, source = self._get_book(coin)

            # Truncate to the top `depth` levels per side. Liquidity follows a
            # power law — the immediate spread carries most of the actionable
            # signal, and truncating slashes token cost.
            levels = result.get("levels")
            if levels and depth > 0:
                result = {
                    **result,
                    "levels": [levels[0][:depth], levels[1][:depth]],
                }

            return {
                "message": f"Order book for {coin} retrieved successfully",
                "data": result,
                "summary": {
                    "coin": coin,
                    "source": source,
                    "depth": depth,
                    "bidsCount": (
                        len(result["levels"][0]) if result.get("levels") else 0
                    ),
                    "asksCount": (
                        len(result["levels"][1]) if result.get("levels") else 0
                    ),
                },
            }

        elif name == "hyperliquid_get_microstructure":
            coin = arguments["coin"]
            depth = arguments.get("depth", 5)

            book, source = self._get_book(coin)
            stats = self._microstructure(book.get("levels") or [[], []], depth)
            if stats is None:
                return {"asset": coin, "error": "empty order book", "source": source}

            # Dense flat signal — no message/data/summary wrapper by design, so
            # the whole response stays ~20 tokens.
            return {"asset": coin, "source": source, "depth": depth, **stats}

        elif name == "hyperliquid_get_orderflow":
            coin = arguments["coin"]
            window_secs = arguments.get("window_secs", 60)

            trades, source = self._get_trades(coin, window_secs)
            stats = self._orderflow(trades, window_secs)
            if stats is None:
                return {
                    "asset": coin,
                    "error": "no trades in window",
                    "source": source,
                    "window_secs": window_secs,
                }

            # Dense flat signal — no message/data/summary wrapper by design.
            return {
                "asset": coin,
                "source": source,
                "window_secs": window_secs,
                **stats,
            }

        elif name == "hyperliquid_get_recent_trades":
            coin = arguments["coin"]
            # Raw post needs the same name_to_coin remap Info methods apply.
            api_coin = self.info.name_to_coin.get(coin, coin)
            result = self.info.post("/info", {"type": "recentTrades", "coin": api_coin})

            return {
                "message": f"Recent trades for {coin} retrieved successfully",
                "data": result,
                "summary": {
                    "coin": coin,
                    "numberOfTrades": len(result) if result else 0,
                },
            }

        elif name == "hyperliquid_get_historical_funding":
            coin = arguments["coin"]
            start_time = int(arguments["startTime"])  # Ensure integer
            end_time = int(arguments["endTime"]) if arguments.get("endTime") else None

            result = self.info.funding_history(
                name=coin, startTime=start_time, endTime=end_time
            )

            return {
                "message": f"Historical funding for {coin} retrieved successfully",
                "data": result,
                "summary": {
                    "coin": coin,
                    "numberOfEntries": len(result) if result else 0,
                },
            }

        elif name == "hyperliquid_get_candles":
            coin = arguments["coin"]
            interval = arguments["interval"]
            start_time = int(arguments["startTime"])  # Ensure integer
            end_time = int(arguments["endTime"]) if arguments.get("endTime") else None

            result = self.info.candles_snapshot(
                name=coin,
                interval=interval,
                startTime=start_time,
                endTime=end_time if end_time is not None else int(time.time() * 1000),
            )

            return {
                "message": f"Candles for {coin} ({interval}) retrieved successfully",
                "data": result,
                "summary": {
                    "coin": coin,
                    "interval": interval,
                    "numberOfCandles": len(result) if result else 0,
                },
            }

        elif name == "hyperliquid_run_monte_carlo":
            coin = arguments["coin"]
            days_forward = arguments.get("days_forward", 7)
            iterations = arguments.get("iterations", 10000)
            interval = arguments.get("interval", "1h")
            lookback_days = arguments.get("lookback_days", 30)
            use_historical_drift = bool(arguments.get("use_historical_drift", False))

            # Steps per day per interval, so `interval` and `days_forward` stay
            # consistent: a 7-day horizon on 1h candles => 7*24 = 168 steps.
            steps_per_day = {
                "1m": 1440,
                "5m": 288,
                "15m": 96,
                "1h": 24,
                "4h": 6,
                "1d": 1,
            }.get(interval)
            if steps_per_day is None:
                return {"asset": coin, "error": f"unsupported interval: {interval}"}
            steps = days_forward * steps_per_day
            if steps <= 0 or iterations <= 0:
                return {
                    "asset": coin,
                    "error": "days_forward and iterations must be positive",
                }
            # Terminal-only sampling is O(iterations); the cap just bounds a
            # runaway request (1M draws is still <10ms and ~8MB).
            iterations = min(iterations, 1_000_000)

            start_time = int(time.time() * 1000) - lookback_days * 86400 * 1000
            candles = self.info.candles_snapshot(
                name=coin,
                interval=interval,
                startTime=start_time,
                endTime=int(time.time() * 1000),
            )
            closes = [c["c"] for c in candles] if candles else []
            if len(closes) < 3:
                return {
                    "asset": coin,
                    "error": "insufficient history",
                    "interval": interval,
                    "candles": len(closes),
                }

            s0 = float(closes[-1])  # last close as the simulation spot price
            stats = self._monte_carlo(
                closes, s0, steps, iterations, use_historical_drift
            )
            if stats is None:
                return {
                    "asset": coin,
                    "error": "insufficient history or zero volatility",
                    "interval": interval,
                    "candles": len(closes),
                }
            return {
                "asset": coin,
                "interval": interval,
                "days_forward": days_forward,
                "lookback_days": lookback_days,
                "drift": "historical" if use_historical_drift else "zero",
                **stats,
            }

        # Vault Management
        elif name == "hyperliquid_vault_details":
            vault_address = arguments["vaultAddress"]
            result = self.info.post(
                "/info", {"type": "vaultDetails", "vaultAddress": vault_address}
            )

            return {
                "message": f"Vault details for {vault_address} retrieved successfully",
                "data": result,
                "vaultAddress": vault_address,
            }

        elif name == "hyperliquid_vault_performance":
            vault_address = arguments["vaultAddress"]

            result = self.info.portfolio(vault_address)

            return {
                "message": f"Vault performance for {vault_address} retrieved successfully",
                "data": result,
                "summary": {"vaultAddress": vault_address},
            }

        # Utility
        elif name == "hyperliquid_get_server_time":
            server_time = int(time.time() * 1000)

            return {
                "message": "Server time retrieved successfully",
                "data": {"serverTime": server_time},
            }

        else:
            raise ValueError(f"Unknown tool: {name}")

    def _parse_order_response(self, result: dict) -> dict:
        """Parse order placement response."""
        order_status = (
            result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
        )
        return self._parse_order_status(order_status)

    def _parse_order_status(self, status: dict) -> dict:
        """Parse a single order status."""
        if "resting" in status:
            return {
                "status": "resting",
                "orderId": status["resting"]["oid"],
                "message": "Order placed and resting on order book",
            }
        elif "filled" in status:
            return {
                "status": "filled",
                "orderId": status["filled"]["oid"],
                "totalSize": status["filled"]["totalSz"],
                "averagePrice": status["filled"]["avgPx"],
                "message": "Order filled successfully",
            }
        elif "error" in status:
            return {
                "status": "error",
                "error": status["error"],
                "message": "Order placement failed",
            }
        else:
            return {"status": "unknown", "rawStatus": status}

    async def run(self):
        """Run the MCP server."""
        async with stdio_server() as (read_stream, write_stream):
            logger.info("Hyperliquid MCP Server started")
            self._start_streams()
            try:
                await self.server.run(
                    read_stream,
                    write_stream,
                    self.server.create_initialization_options(),
                )
            finally:
                # Stop the SDK's WebSocket + ping threads (non-daemon) so the
                # process can exit cleanly.
                try:
                    self.info.disconnect_websocket()
                except Exception:
                    pass


def main():
    """Main entry point."""
    try:
        server = HyperliquidMCPServer()
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
