"""Hyperliquid MCP Server - Main implementation."""

import asyncio
import json
import logging
import math
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
        # Which builder (HIP-3) perp dexes to preload alongside the primary dex.
        # Loading a dex's universe costs one REST round-trip each, and the
        # network now exposes hundreds of them (237 on testnet), so eager-loading
        # all of them adds ~90s to startup and blows past the MCP client's 30s
        # connect timeout. Default to the primary dex only (fast); opt in with a
        # comma-separated list of dex names, or "all" for the old behavior.
        self.perp_dexs_config = os.getenv("HYPERLIQUID_PERP_DEXS", "").strip()

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
        self.local_asset_ctx: dict[str, dict] = (
            {}
        )  # coin -> {"ctx", "received_at"} live asset context (incl. openInterest)
        self._subscribed_asset_ctx: set[str] = (
            set()
        )  # coins with a live activeAssetCtx subscription

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

            # Resolve which perp dexes to load. The primary dex ("") is always
            # loaded; builder-deployed HIP-3 dexes (e.g. the one hosting equity/
            # commodity perps like META, AAPL, gold) are loaded only when opted
            # into via HYPERLIQUID_PERP_DEXS, because loading every discovered
            # dex's universe (237 on testnet) serially adds ~90s to startup.
            #   unset/empty -> primary dex only (fast default)
            #   "all"       -> every discovered dex (old behavior; slow)
            #   "a,b,c"     -> primary dex plus the named dexes
            cfg = self.perp_dexs_config.lower()
            if cfg == "all":
                bootstrap_info = Info(base_url, skip_ws=True)
                perp_dex_list = bootstrap_info.perp_dexs()
                dex_names = [""] + [d["name"] for d in perp_dex_list[1:] if d]
            elif self.perp_dexs_config:
                requested = [
                    d.strip() for d in self.perp_dexs_config.split(",") if d.strip()
                ]
                dex_names = [""] + [d for d in requested if d]
            else:
                dex_names = [""]
            logger.info(f"Loading perp dexes: {dex_names}")

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

    def _on_asset_ctx(self, coin: str, msg: dict) -> None:
        """WS callback: mirror the latest activeAssetCtx (incl. openInterest).

        The activeAssetCtx stream pushes a full ctx snapshot per message
        ({"data": {"coin", "ctx": {...}}}), so — like l2Book — maintaining the
        mirror is just replacing the coin's entry. `coin` is captured via a
        per-subscription closure so the entry is keyed by the exact requested
        name (sidesteps any WS name remap).
        """
        data = msg.get("data") or {}
        ctx = data.get("ctx")
        if not ctx:
            return
        with self._state_lock:
            self.local_asset_ctx[coin] = {"ctx": ctx, "received_at": time.time()}

    def _ensure_asset_ctx_subscription(self, coin: str) -> None:
        """Subscribe to activeAssetCtx for coin once; safe to call repeatedly."""
        with self._state_lock:
            if coin in self._subscribed_asset_ctx:
                return
            self._subscribed_asset_ctx.add(coin)
        try:
            self.info.subscribe(
                {"type": "activeAssetCtx", "coin": coin},
                lambda m, c=coin: self._on_asset_ctx(c, m),
            )
        except Exception as e:
            logger.warning(f"Failed to subscribe activeAssetCtx for {coin}: {e}")
            with self._state_lock:
                self._subscribed_asset_ctx.discard(coin)  # allow retry next call

    def _asset_ctx_rest(self, coin: str, dex: str) -> Optional[dict]:
        """REST pull of a single asset's live ctx (incl. openInterest).

        metaAndAssetCtxs takes no coin, so the coin is matched against the
        returned universe: by name, by name_to_coin remap, or by the bare name
        after stripping a "dex:" prefix (builder-dex universes list bare names).
        Returns None when the coin isn't on the dex.
        """
        meta, ctxs = self.info.post("/info", {"type": "metaAndAssetCtxs", "dex": dex})
        universe = meta["universe"]
        target = self.info.name_to_coin.get(coin, coin)
        bare = coin.split(":")[-1]
        for idx, asset in enumerate(universe):
            if idx >= len(ctxs):
                break
            if asset["name"] in (coin, target, bare):
                return ctxs[idx]
        return None

    def _get_asset_ctx(
        self, coin: str, dex: str = "", stale_secs: float = 5.0
    ) -> tuple[Optional[dict], str]:
        """Return (ctx, source) for a coin's live context (openInterest, funding,
        mark/oracle/mid px, day volume).

        Serves the activeAssetCtx WS mirror when fresh (< stale_secs old), else a
        REST metaAndAssetCtxs pull. Like the book, activeAssetCtx pushes on a
        regular (per-block) cadence, so plain age-based staleness is the right
        freshness test (unlike the trade tape, which can be legitimately silent).
        `ctx` is None only when the coin is unknown on the dex.

        The WS mirror is used for the primary dex only; a non-default `dex` read
        goes straight to REST (freshness matters most for the hot primary assets,
        and this dodges any builder-dex WS name-resolution ambiguity).
        """
        if not dex:
            self._ensure_asset_ctx_subscription(coin)
            with self._state_lock:
                cached = self.local_asset_ctx.get(coin)
            if cached and (time.time() - cached["received_at"]) < stale_secs:
                return cached["ctx"], "websocket"
        return self._asset_ctx_rest(coin, dex), "rest"

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

    @staticmethod
    def _align_candles(asset_candles: list, benchmark_candles: list) -> tuple:
        """Pair two candle series by shared open-time, returning aligned closes.

        Beta is a regression of one return series on another, so the two series
        must be sampled at the **same instants** or the slope is meaningless.
        Candles carry an open-time under key ``"t"`` and a close under ``"c"``
        (same keys the indicators/monte-carlo handlers read). This intersects on
        ``t``, sorts ascending, and emits ``(asset_closes, bench_closes)`` as
        equal-length float-string lists. Both perps trade continuously so the
        overlap is ~total; the intersection cleanly drops the rare missing
        candle. Returns ``([], [])`` when there is no shared timestamp.
        """
        a_by_t = {c["t"]: c["c"] for c in (asset_candles or [])}
        b_by_t = {c["t"]: c["c"] for c in (benchmark_candles or [])}
        common = sorted(set(a_by_t) & set(b_by_t))
        return [a_by_t[t] for t in common], [b_by_t[t] for t in common]

    @staticmethod
    def _beta(asset_closes: list, benchmark_closes: list) -> Optional[dict]:
        """Market beta of an asset vs. a benchmark, from one log-return regression.

        `asset_closes`/`benchmark_closes` are **timestamp-aligned** equal-length
        chronological close series (strings or floats — align via `_align_candles`
        first). All figures fall out of regressing the asset's log returns on the
        benchmark's:

            beta = Cov(asset_ret, bench_ret) / Var(bench_ret)

        beta is the sensitivity (1 = moves 1:1 with the market, >1 amplified,
        <0 inverse); `correlation` is the confidence gate on that slope (a big
        beta with low rho is noise); `r_squared` is the share of the asset's
        variance the benchmark explains; the two per-step vols give the context
        that makes beta legible (beta = asset_vol/bench_vol * correlation).

        Returns None on bad input — unequal lengths, fewer than 3 closes, any
        non-positive price (log undefined), or a benchmark with zero/non-finite
        variance (nothing to regress against). If the *asset* is flat (zero
        variance), beta is a well-defined 0 but correlation/r_squared are
        undefined and returned as None. Every scalar is coerced to a Python
        float — numpy's np.float64 would break json.dumps (same footgun as _rsi).
        """
        a = np.asarray([float(x) for x in asset_closes], dtype=float)
        b = np.asarray([float(x) for x in benchmark_closes], dtype=float)
        if a.size != b.size or a.size < 3 or (a <= 0).any() or (b <= 0).any():
            return None

        ra = np.diff(np.log(a))
        rb = np.diff(np.log(b))
        var_b = float(rb.var(ddof=1))
        if var_b == 0.0 or not np.isfinite(var_b):
            return None

        asset_vol = float(ra.std(ddof=1))
        bench_vol = float(rb.std(ddof=1))
        cov = float(np.cov(ra, rb, ddof=1)[0, 1])
        beta = cov / var_b

        # corr is undefined when the asset is flat (0/0 -> nan); beta stays 0.
        if asset_vol == 0.0:
            correlation = None
            r_squared = None
        else:
            correlation = float(np.corrcoef(ra, rb)[0, 1])
            r_squared = correlation**2

        return {
            "beta": round(beta, 6),
            "correlation": round(correlation, 6) if correlation is not None else None,
            "r_squared": round(r_squared, 6) if r_squared is not None else None,
            "asset_vol_per_step": round(asset_vol, 8),
            "benchmark_vol_per_step": round(bench_vol, 8),
            "observations": int(ra.size),
        }

    @staticmethod
    def _ema(arr: np.ndarray, period: int) -> Optional[float]:
        """Canonical exponential moving average of `arr`, latest value only.

        Seeded with the SMA of the first `period` samples, then smoothed forward
        with k = 2/(period+1). Returns None if there aren't `period` samples.
        Reusable "subroutine" — call it per interval to build multi-timeframe EMAs.
        """
        n = arr.size
        if n < period or period <= 0:
            return None
        k = 2.0 / (period + 1)
        ema = float(arr[:period].mean())  # SMA seed
        for x in arr[period:]:
            ema = float(x) * k + ema * (1 - k)
        return ema

    @staticmethod
    def _rsi(closes: np.ndarray, period: int) -> Optional[float]:
        """Wilder's RSI (RMA smoothing), latest value only, in [0, 100].

        Seeds the average gain/loss over the first `period` deltas, then applies
        Wilder's smoothing forward. Returns None if there are fewer than
        `period + 1` closes. Edge cases: all-gains -> 100, all-losses -> 0.
        """
        n = closes.size
        if n < period + 1 or period <= 0:
            return None
        delta = np.diff(closes)
        gains = np.where(delta > 0, delta, 0.0)
        losses = np.where(delta < 0, -delta, 0.0)
        avg_gain = float(gains[:period].mean())
        avg_loss = float(losses[:period].mean())
        for i in range(period, delta.size):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0.0:
            return 100.0 if avg_gain > 0.0 else 50.0  # flat series -> neutral
        if avg_gain == 0.0:
            return 0.0
        rs = avg_gain / avg_loss
        return float(100.0 - 100.0 / (1.0 + rs))

    @staticmethod
    def _indicators(
        closes: list,
        volumes: list,
        *,
        rsi_period: int = 14,
        bb_period: int = 20,
        bb_stddev: float = 2.0,
        vol_sma_period: int = 20,
        ema_periods: tuple = (9, 21, 50, 200),
    ) -> Optional[dict]:
        """Discretionary indicator bundle: RSI, Bollinger Bands, EMAs, Volume SMA.

        `closes`/`volumes` are chronological series (strings or floats). Returns a
        nested dict pairing each indicator's raw value with **deterministic** flags
        (mathematical facts, never opinion) so a small model can branch on booleans
        instead of doing float math. An indicator whose window exceeds the data
        yields a null `value` (rather than failing the whole call); returns None
        only when there isn't even enough data for the smallest indicator.
        """
        c = np.asarray([float(x) for x in closes], dtype=float)
        v = np.asarray([float(x) for x in volumes], dtype=float)
        if c.size < rsi_period + 1 or (c <= 0).any():
            return None
        price = float(c[-1])

        # --- RSI ---
        rsi_val = HyperliquidMCPServer._rsi(c, rsi_period)
        rsi: dict[str, Any] = {
            "value": round(rsi_val, 4) if rsi_val is not None else None,
            "period": rsi_period,
            "zone": None,
            "is_overbought": None,
            "is_oversold": None,
        }
        if rsi_val is not None:
            rsi["is_overbought"] = rsi_val > 70
            rsi["is_oversold"] = rsi_val < 30
            rsi["zone"] = (
                "OVERBOUGHT"
                if rsi_val > 70
                else "OVERSOLD" if rsi_val < 30 else "NEUTRAL"
            )

        # --- Bollinger Bands (population std, ddof=0 — the charting convention) ---
        bb: dict = {
            "period": bb_period,
            "stddev": bb_stddev,
            "upper": None,
            "middle": None,
            "lower": None,
            "percent_b": None,
            "bandwidth": None,
            "price_vs_bands": None,
            "is_squeeze": None,
        }
        if c.size >= bb_period and bb_period > 0:
            window = c[-bb_period:]
            middle = float(window.mean())
            sd = float(window.std(ddof=0))
            upper = middle + bb_stddev * sd
            lower = middle - bb_stddev * sd
            bb["middle"] = round(middle, 8)
            bb["upper"] = round(upper, 8)
            bb["lower"] = round(lower, 8)
            bb["bandwidth"] = (
                round((upper - lower) / middle, 8) if middle != 0 else None
            )
            bb["percent_b"] = (
                round((price - lower) / (upper - lower), 6) if upper != lower else None
            )
            bb["price_vs_bands"] = (
                "ABOVE_UPPER"
                if price > upper
                else "BELOW_LOWER" if price < lower else "INSIDE"
            )
            # Deterministic squeeze: current bandwidth is at its trailing minimum.
            bandwidths = []
            for i in range(bb_period, c.size + 1):
                w = c[i - bb_period : i]
                mu = float(w.mean())
                if mu != 0:
                    bandwidths.append(2 * bb_stddev * float(w.std(ddof=0)) / mu)
            bb["is_squeeze"] = (
                bool(bandwidths and bandwidths[-1] <= min(bandwidths))
                if bandwidths
                else None
            )

        # --- EMAs (reusable subroutine per length) ---
        ema: dict[str, Any] = {"is_ordered_up": None, "is_ordered_down": None}
        ema_vals = {}
        for p in ema_periods:
            val = HyperliquidMCPServer._ema(c, int(p))
            ema_vals[int(p)] = val
            ema[str(int(p))] = {
                "value": round(val, 8) if val is not None else None,
                "price_is_above": (price > val) if val is not None else None,
            }
        ordered = [ema_vals[int(p)] for p in ema_periods]
        if all(x is not None for x in ordered):
            o = [float(x) for x in ordered if x is not None]
            ema["is_ordered_up"] = all(o[i] > o[i + 1] for i in range(len(o) - 1))
            ema["is_ordered_down"] = all(o[i] < o[i + 1] for i in range(len(o) - 1))

        # --- Volume SMA ---
        vol: dict = {
            "period": vol_sma_period,
            "current": None,
            "sma": None,
            "ratio": None,
            "above_sma": None,
        }
        if v.size >= 1:
            vol["current"] = round(float(v[-1]), 8)
        if v.size >= vol_sma_period and vol_sma_period > 0:
            sma = float(v[-vol_sma_period:].mean())
            vol["sma"] = round(sma, 8)
            if sma != 0:
                vol["ratio"] = round(float(v[-1]) / sma, 4)
                vol["above_sma"] = float(v[-1]) > sma

        return {
            "price": round(price, 8),
            "rsi": rsi,
            "bollinger": bb,
            "ema": ema,
            "volume": vol,
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
                Tool(
                    name="hyperliquid_update_leverage",
                    description="Set the leverage AND margin mode for a perp asset. update_leverage configures both at once: the leverage multiplier and whether the asset uses cross margin (isCross=true) or isolated margin (isCross=false). Leverage is capped at the asset's maxLeverage (see hyperliquid_get_meta); the exchange rejects out-of-range values and the error is surfaced.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "asset": {
                                "type": "integer",
                                "description": "Asset index (e.g. 0 for BTC). See hyperliquid_get_meta for the index<->coin mapping.",
                            },
                            "leverage": {
                                "type": "integer",
                                "description": "Leverage multiplier (e.g. 5 for 5x). Must be <= the asset's maxLeverage.",
                            },
                            "isCross": {
                                "type": "boolean",
                                "description": "Margin mode: true = cross margin (default), false = isolated margin.",
                                "default": True,
                            },
                        },
                        "required": ["asset", "leverage"],
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
                    name="hyperliquid_get_open_interest",
                    description="Get open interest and live market context (funding rate, mark/oracle/mid price, 24h volume) for a perp asset. Open interest is returned in both base units and USD notional (base * mark price). Omit 'coin' to get a list for every asset on the dex, sorted by notional OI. Defaults to the main perp dex; pass 'dex' for a builder-deployed dex.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'SOL'). For builder-dex assets use the dex-prefixed name (e.g. 'xyz:META'). Omit to return all assets on the dex.",
                            },
                            "dex": {
                                "type": "string",
                                "description": "Perp dex name (optional, defaults to the main dex)",
                                "default": "",
                            },
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
                    name="hyperliquid_get_indicators",
                    description="Compute the classic discretionary technical-indicator bundle (RSI, Bollinger Bands, EMA 9/21/50/200, Volume SMA) server-side from recent candles, in one call. Returns raw values AND deterministic derived flags (e.g. rsi_zone, price_is_above each EMA, price_vs_bands, is_ordered_up) - mathematical facts only, never trading opinions - so a downstream model branches on booleans instead of doing float math. Single interval per call: call twice (e.g. '1h' then '1d') for multi-timeframe context.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol (e.g., 'BTC', 'ETH', 'SOL'). For builder-deployed dex assets like equities/commodities, use the dex-prefixed name (e.g. 'xyz:META' for Meta Platforms) - see hyperliquid_get_perp_dexs and hyperliquid_get_meta.",
                            },
                            "interval": {
                                "type": "string",
                                "description": "Candle interval to compute indicators on (optional, default '1h')",
                                "enum": ["1m", "5m", "15m", "1h", "4h", "1d"],
                                "default": "1h",
                            },
                            "rsi_period": {
                                "type": "integer",
                                "description": "RSI lookback period (optional, default 14)",
                                "default": 14,
                            },
                            "bb_period": {
                                "type": "integer",
                                "description": "Bollinger Bands SMA period (optional, default 20)",
                                "default": 20,
                            },
                            "bb_stddev": {
                                "type": "number",
                                "description": "Bollinger Bands standard-deviation multiplier (optional, default 2)",
                                "default": 2,
                            },
                            "vol_sma_period": {
                                "type": "integer",
                                "description": "Volume SMA period (optional, default 20)",
                                "default": 20,
                            },
                        },
                        "required": ["coin"],
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
                Tool(
                    name="hyperliquid_get_beta",
                    description="Compute an asset's market beta against a benchmark from a single log-return regression over aligned candles. Beta = Cov(asset,benchmark)/Var(benchmark): the asset's sensitivity to the benchmark (1 = moves 1:1, >1 amplified, <0 inverse) - useful for position sizing and spotting when two positions are secretly the same market bet. Returns descriptive coupling facts only (beta, correlation, r_squared, per-step vols), never trading opinions. Benchmark is required (commonly 'BTC' as the crypto market proxy); there is no implicit default.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "coin": {
                                "type": "string",
                                "description": "Asset symbol to measure (e.g., 'ETH', 'SOL'). For builder-deployed dex assets use the dex-prefixed name (e.g. 'xyz:META') - see hyperliquid_get_perp_dexs and hyperliquid_get_meta.",
                            },
                            "benchmark": {
                                "type": "string",
                                "description": "Benchmark symbol to regress against, i.e. 'the market' (e.g. 'BTC'). Required - there is no implicit default.",
                            },
                            "interval": {
                                "type": "string",
                                "description": "Candle interval used to compute returns (optional, default '1h')",
                                "enum": ["1m", "5m", "15m", "1h", "4h", "1d"],
                                "default": "1h",
                            },
                            "lookback_days": {
                                "type": "integer",
                                "description": "How many days of history to estimate beta over (optional, default 30)",
                                "default": 30,
                            },
                        },
                        "required": ["coin", "benchmark"],
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
            "leverage",
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
            "rsi_period",
            "bb_period",
            "vol_sma_period",
        ]
        for param in integer_params:
            if param in arguments and arguments[param] is not None:
                arguments[param] = self._coerce_int(param, arguments[param])

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
            # Hyperliquid keeps two independent ledgers: the PERP account
            # (clearinghouseState) and the SPOT account (spotClearinghouseState).
            # user_state only sees perps, so a wallet whose funds sit in spot
            # reports a $0 perp balance. Query both and surface them together.
            result = self.info.user_state(user_address, dex=dex)
            margin_summary = result["marginSummary"]
            perp_withdrawable = result["withdrawable"]

            # Spot balances (list of {coin, token, total, hold, entryNtl}); spot
            # is dex-independent, so it's queried once regardless of `dex`.
            spot_balances: list = []
            spot_usdc = "0.0"
            try:
                spot_state = self.info.spot_user_state(user_address)
                spot_balances = spot_state.get("balances", []) or []
                for bal in spot_balances:
                    if bal.get("coin") == "USDC":
                        spot_usdc = bal.get("total", "0.0")
            except Exception as e:
                logger.warning(f"Failed to fetch spot balance: {e}")

            # Non-zero spot holdings, keyed by coin, for a compact summary view.
            spot_summary = {
                b["coin"]: b["total"]
                for b in spot_balances
                if float(b.get("total", "0") or "0") > 0
            }
            perp_available = float(margin_summary["accountValue"]) - float(
                margin_summary["totalMarginUsed"]
            )

            return {
                "message": "Balance retrieved successfully",
                "data": {
                    "perp": {
                        "accountValue": margin_summary["accountValue"],
                        "totalMarginUsed": margin_summary["totalMarginUsed"],
                        "totalNtlPos": margin_summary["totalNtlPos"],
                        "totalRawUsd": margin_summary["totalRawUsd"],
                        "withdrawable": perp_withdrawable,
                    },
                    "spot": {
                        "usdc": spot_usdc,
                        "balances": spot_balances,
                    },
                },
                "summary": {
                    "perpAccountValue": margin_summary["accountValue"],
                    "perpWithdrawable": perp_withdrawable,
                    "perpAvailableBalance": str(perp_available),
                    "spotUsdc": spot_usdc,
                    "spotHoldings": spot_summary,
                    # Cash you could trade with right now: free perp margin +
                    # spot USDC (spot has to be transferred to perp to trade perps).
                    "totalUsdcAcrossAccounts": str(
                        perp_available + float(spot_usdc or "0")
                    ),
                },
            }

        elif name == "hyperliquid_update_leverage":
            asset = arguments["asset"]  # Already normalized to integer
            leverage = arguments["leverage"]  # Already normalized to integer
            is_cross = arguments.get("isCross", True)

            # Convert asset index to coin name (same guard as place_order)
            coin_name = self.asset_index_to_name.get(asset)
            if coin_name is None:
                raise ValueError(f"Unknown asset index: {asset}")

            result = self.exchange.update_leverage(leverage, coin_name, is_cross)

            # Surface a request-level rejection (e.g. leverage above maxLeverage)
            # cleanly instead of returning an opaque {"status": "err"} blob.
            err = self._top_level_error(result)
            if err is not None:
                return {"error": err, "requestParams": arguments}

            mode = "cross" if is_cross else "isolated"
            return {
                "message": f"Leverage set to {leverage}x ({mode}) for {coin_name}",
                "data": result,
                "summary": {
                    "asset": asset,
                    "coin": coin_name,
                    "leverage": leverage,
                    "marginMode": mode,
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

            # normalTpsl ties the TP and SL to the entry as a native OCO pair:
            # when one fills the exchange auto-cancels the sibling, and
            # cancelling the entry cancels both. Without this (grouping defaults
            # to "na") the three orders are independent and a filled side leaves
            # its stale sibling resting on the book.
            result = self.exchange.bulk_orders(orders, grouping="normalTpsl")

            # Surface a top-level rejection (unapproved agent wallet,
            # insufficient margin, ...) instead of crashing in the parse
            # chain: on failure `response` is a string, not a statuses dict.
            err = self._top_level_error(result)
            if err is not None:
                return {
                    "message": "Bracket order placement failed",
                    "error": err,
                    "data": result,
                    "requestParams": arguments,
                }

            # Parse per-leg statuses: the exchange can return status "ok"
            # with individual legs rejected, and an entry that fills with a
            # rejected SL leg is an unprotected position — that must never
            # read as "placed successfully".
            order_infos, failed_legs = self._parse_bracket_result(result)
            if failed_legs:
                return {
                    "message": "Bracket order partially failed",
                    "error": "; ".join(
                        f"{leg['orderType']}: {leg['error']}" for leg in failed_legs
                    ),
                    "data": result,
                    "orders": order_infos,
                    "requestParams": arguments,
                }

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

            summary = self._parse_cancel_result(result, [{"coin": coin, "oid": oid}])
            if summary["failedCount"]:
                return {
                    "message": f"Cancel failed for order {oid} ({coin})",
                    "error": summary.get("error", "unrecognized cancel status"),
                    "data": result,
                    "requestParams": arguments,
                }

            return {
                "message": f"Order {oid} cancelled for {coin}",
                "data": result,
                "cancelledOrder": {"coin": coin, "orderId": oid},
            }

        elif name == "hyperliquid_cancel_all_orders":
            dex = arguments.get("dex", "")

            # frontend_open_orders (not open_orders) so untriggered TP/SL
            # trigger orders are included — "cancel all" that leaves stale
            # stops resting would fire them later against a flat book.
            open_orders = self.info.frontend_open_orders(user_address, dex=dex)

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

            # An order can legitimately fill between the open-orders fetch and
            # the cancel — report per-oid outcomes so the caller re-checks
            # instead of assuming everything it saw is now gone.
            summary = self._parse_cancel_result(result, cancel_requests)
            response = {
                "message": (
                    f"Cancelled {summary['cancelledCount']} of "
                    f"{len(cancel_requests)} orders"
                ),
                "data": result,
                "cancelledCount": summary["cancelledCount"],
                "failedCount": summary["failedCount"],
                "outcomes": summary["outcomes"],
            }
            if "error" in summary:
                response["error"] = summary["error"]
            return response

        elif name == "hyperliquid_modify_order":
            oid = arguments["oid"]  # Already normalized to integer
            coin = arguments["coin"]
            is_buy = arguments["isBuy"]
            size = self._positive_float("size", arguments["size"])
            price = self._positive_float("price", arguments["price"])
            reduce_only = arguments.get("reduceOnly", False)
            order_type = arguments.get("orderType", {"limit": {"tif": "Gtc"}})

            # Same coercion as place_order: a string triggerPx would die deep
            # in the SDK's float_to_wire with an opaque TypeError.
            if "trigger" in order_type:
                trigger = order_type["trigger"]
                if "triggerPx" in trigger and isinstance(trigger["triggerPx"], str):
                    trigger["triggerPx"] = float(trigger["triggerPx"])

            result = self.exchange.modify_order(
                oid=oid,
                name=coin,
                is_buy=is_buy,
                sz=size,
                limit_px=price,
                order_type=order_type,
                reduce_only=reduce_only,
            )

            # A rejected modify must never read as success: the caller may be
            # "moving a stop" and would otherwise believe the position is
            # protected at the new price.
            order_info = self._parse_order_response(result)
            if order_info["status"] == "error":
                return {
                    "message": f"Order {oid} modification failed",
                    "error": order_info["error"],
                    "data": result,
                    "requestParams": arguments,
                }

            return {
                "message": f"Order {oid} modified successfully",
                "data": result,
                "orderInfo": order_info,
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

        elif name == "hyperliquid_get_open_interest":
            dex = arguments.get("dex", "")
            coin = arguments.get("coin")

            def _oi_row(asset_name: str, ctx: dict) -> dict:
                oi_base = float(ctx.get("openInterest", "0") or "0")
                # Notional = base OI * mark px (fall back to mid/oracle).
                px = ctx.get("markPx") or ctx.get("midPx") or ctx.get("oraclePx")
                px_f = float(px) if px else 0.0
                return {
                    "coin": asset_name,
                    "openInterest": ctx.get("openInterest"),
                    "openInterestNotional": round(oi_base * px_f, 2),
                    "funding": ctx.get("funding"),
                    "markPx": ctx.get("markPx"),
                    "oraclePx": ctx.get("oraclePx"),
                    "midPx": ctx.get("midPx"),
                    "dayNtlVlm": ctx.get("dayNtlVlm"),
                    "prevDayPx": ctx.get("prevDayPx"),
                }

            if coin:
                # Single asset: served from the activeAssetCtx WS mirror when
                # fresh (O(1), no REST), else a REST metaAndAssetCtxs pull.
                ctx, source = self._get_asset_ctx(coin, dex)
                if ctx is None:
                    return {
                        "error": f"Unknown coin '{coin}' on dex '{dex or 'main'}'. Use hyperliquid_get_meta to list assets."
                    }
                row = _oi_row(coin, ctx)
                return {
                    "message": f"Open interest for {coin} retrieved successfully",
                    "data": {**row, "source": source},
                    "summary": {
                        "coin": coin,
                        "openInterest": row["openInterest"],
                        "openInterestNotional": row["openInterestNotional"],
                        "funding": row["funding"],
                        "markPx": row["markPx"],
                        "source": source,
                    },
                }

            # All assets on the dex: one bulk REST pull (the WS mirror is
            # per-coin, so it can't serve a full-universe scan). openInterest
            # lives in metaAndAssetCtxs, not meta; post directly with `dex` so
            # builder dexes work (mirrors how meta(dex=) does it).
            meta, ctxs = self.info.post(
                "/info", {"type": "metaAndAssetCtxs", "dex": dex}
            )
            universe = meta["universe"]
            rows = [
                _oi_row(asset["name"], ctxs[idx])
                for idx, asset in enumerate(universe)
                if idx < len(ctxs)
            ]
            rows.sort(key=lambda r: r["openInterestNotional"], reverse=True)
            return {
                "message": "Open interest for all assets retrieved successfully",
                "data": rows,
                "summary": {
                    "dex": dex,
                    "numberOfAssets": len(rows),
                    "totalOpenInterestNotional": round(
                        sum(r["openInterestNotional"] for r in rows), 2
                    ),
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

        elif name == "hyperliquid_get_indicators":
            coin = arguments["coin"]
            interval = arguments.get("interval", "1h")
            rsi_period = arguments.get("rsi_period", 14)
            bb_period = arguments.get("bb_period", 20)
            bb_stddev = float(arguments.get("bb_stddev", 2))
            vol_sma_period = arguments.get("vol_sma_period", 20)

            # Fetch enough history to seed the 200-EMA well (~3x its length is a
            # common convergence rule); the horizon and estimation interval match.
            minutes_per = {
                "1m": 1,
                "5m": 5,
                "15m": 15,
                "1h": 60,
                "4h": 240,
                "1d": 1440,
            }.get(interval)
            if minutes_per is None:
                return {"asset": coin, "error": f"unsupported interval: {interval}"}
            now_ms = int(time.time() * 1000)
            start_time = now_ms - 600 * minutes_per * 60_000

            candles = self.info.candles_snapshot(
                name=coin,
                interval=interval,
                startTime=start_time,
                endTime=now_ms,
            )
            closes = [c["c"] for c in candles] if candles else []
            volumes = [c["v"] for c in candles] if candles else []
            if len(closes) < 3:
                return {
                    "asset": coin,
                    "error": "insufficient history",
                    "interval": interval,
                    "candles": len(closes),
                }

            stats = self._indicators(
                closes,
                volumes,
                rsi_period=rsi_period,
                bb_period=bb_period,
                bb_stddev=bb_stddev,
                vol_sma_period=vol_sma_period,
            )
            if stats is None:
                return {
                    "asset": coin,
                    "error": "insufficient history",
                    "interval": interval,
                    "candles": len(closes),
                }
            # Dense nested dict - no message/data/summary wrapper by design: raw
            # values paired with deterministic flags for cheap downstream branching.
            return {
                "asset": coin,
                "interval": interval,
                "candles": len(closes),
                **stats,
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

        elif name == "hyperliquid_get_beta":
            coin = arguments["coin"]
            benchmark = arguments["benchmark"]
            interval = arguments.get("interval", "1h")
            lookback_days = arguments.get("lookback_days", 30)

            now_ms = int(time.time() * 1000)
            start_time = now_ms - lookback_days * 86400 * 1000
            # Fetch both series over the same window/interval; align on shared
            # candle open-times so the regression compares like-for-like instants.
            asset_candles = self.info.candles_snapshot(
                name=coin,
                interval=interval,
                startTime=start_time,
                endTime=now_ms,
            )
            bench_candles = self.info.candles_snapshot(
                name=benchmark,
                interval=interval,
                startTime=start_time,
                endTime=now_ms,
            )
            asset_closes, bench_closes = self._align_candles(
                asset_candles, bench_candles
            )
            if len(asset_closes) < 3:
                return {
                    "asset": coin,
                    "benchmark": benchmark,
                    "error": "insufficient overlapping history",
                    "interval": interval,
                    "candles": len(asset_closes),
                }

            stats = self._beta(asset_closes, bench_closes)
            if stats is None:
                return {
                    "asset": coin,
                    "benchmark": benchmark,
                    "error": "insufficient history or zero benchmark volatility",
                    "interval": interval,
                    "candles": len(asset_closes),
                }
            # Flat dense dict (no message/data/summary wrapper) like run_monte_carlo:
            # already-normalized risk scalars that speak for themselves.
            return {
                "asset": coin,
                "benchmark": benchmark,
                "interval": interval,
                "lookback_days": lookback_days,
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

    @staticmethod
    def _coerce_int(name: str, value: Any) -> int:
        """Coerce a numeric argument to int, rejecting non-integral values.

        Clients sometimes send integers as floats ("5.0"), which is fine —
        but truncating a genuinely fractional value would silently target
        the wrong thing (asset 5.7 -> the wrong instrument, oid 123.9 ->
        the wrong order), so those raise instead.
        """
        try:
            f = float(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"Invalid {name} parameter: {value!r}. Must be a valid integer."
            )
        if not math.isfinite(f) or f != int(f):
            raise ValueError(
                f"Invalid {name} parameter: {value!r}. Must be a whole number."
            )
        return int(f)

    @staticmethod
    def _positive_float(name: str, value: Any) -> float:
        """Coerce a size/price argument to a finite positive float.

        This must run before anything reaches signing: the SDK's
        float_to_wire guard (``abs(x) >= 1e-12``) is False for NaN, so a
        "NaN" size would be signed and posted verbatim, and inf/negative
        values likewise rely entirely on exchange-side rejection.
        """
        try:
            v = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid {name}: {value!r}. Must be a number.")
        if not math.isfinite(v) or v <= 0:
            raise ValueError(
                f"Invalid {name}: {value!r}. Must be a finite positive number."
            )
        return v

    @staticmethod
    def _top_level_error(result: Any) -> Optional[str]:
        """Extract a top-level API failure message, if any.

        On a request-level rejection (bad/unapproved agent wallet,
        insufficient margin, min order value, ...) the exchange returns
        ``{"status": "err", "response": "<error string>"}`` — ``response``
        is a *string*, not the usual ``{"data": {"statuses": [...]}}`` dict.
        Blindly calling ``.get("data")`` on it raises AttributeError, which
        would mask the real error behind an opaque parse crash. Returns the
        error string when the result is such a failure (or not a dict at
        all), else None.
        """
        if not isinstance(result, dict):
            return str(result)
        if result.get("status") == "err":
            resp = result.get("response")
            return resp if isinstance(resp, str) else str(resp)
        return None

    def _parse_order_response(self, result: dict) -> dict:
        """Parse order placement response."""
        err = self._top_level_error(result)
        if err is not None:
            return {
                "status": "error",
                "error": err,
                "message": "Order placement failed",
            }
        order_status = (
            result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
        )
        return self._parse_order_status(order_status)

    def _parse_cancel_result(self, result: Any, requested: list) -> dict:
        """Summarize a cancel/bulk_cancel response against the requested orders.

        The exchange reports per-order outcomes in ``statuses`` — the string
        ``"success"`` or ``{"error": "..."}`` (e.g. "Order was never placed,
        already canceled, or filled.") — and can also fail top-level. A cancel
        that silently failed leaves a live order the caller believes is gone,
        so anything that isn't an explicit success is counted as failed.
        """
        err = self._top_level_error(result)
        if err is not None:
            return {
                "cancelledCount": 0,
                "failedCount": len(requested),
                "error": err,
                "outcomes": [
                    {**req, "status": "error", "error": err} for req in requested
                ],
            }
        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
        outcomes = []
        cancelled = 0
        errors = []
        for i, req in enumerate(requested):
            status = statuses[i] if i < len(statuses) else None
            if status == "success":
                cancelled += 1
                outcomes.append({**req, "status": "success"})
            elif isinstance(status, dict) and "error" in status:
                errors.append(f"oid {req.get('oid')}: {status['error']}")
                outcomes.append({**req, "status": "error", "error": status["error"]})
            else:
                errors.append(f"oid {req.get('oid')}: unrecognized cancel status")
                outcomes.append({**req, "status": "unknown", "rawStatus": status})
        summary = {
            "cancelledCount": cancelled,
            "failedCount": len(requested) - cancelled,
            "outcomes": outcomes,
        }
        if errors:
            summary["error"] = "; ".join(errors)
        return summary

    def _parse_bracket_result(self, result: dict) -> tuple:
        """Parse bulk_orders bracket statuses into (order_infos, failed_legs).

        Statuses map positionally to the submitted order list (entry first).
        The exchange can return status "ok" with per-leg {"error": ...}
        entries, so each leg must be inspected — an entry that fills while
        its SL leg was rejected is an unprotected position.
        """
        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
        leg_names = ["entry", "take-profit", "stop-loss"]
        order_infos = []
        for idx, status in enumerate(statuses):
            info = self._parse_order_status(status)
            info["orderType"] = leg_names[idx] if idx < len(leg_names) else f"leg-{idx}"
            order_infos.append(info)
        failed_legs = [i for i in order_infos if i["status"] == "error"]
        return order_infos, failed_legs

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
