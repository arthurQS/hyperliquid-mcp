"""Hyperliquid MCP Server - Main implementation."""

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import re
import sqlite3
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    tomllib = None

import eth_account
import numpy as np
import requests
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

# Finite timeout for every SDK REST call. The SDK defaults to timeout=None,
# which requests treats as wait-forever — one hung connection would pin an
# executor thread for the life of the process, and a few of them starve the
# to_thread pool and hang the whole server mid-trade. Caveat for write calls:
# a timeout does NOT mean the order failed — it may have executed after the
# request was sent; recheck order status before retrying (see call_tool).
HTTP_TIMEOUT_SECS = 10.0

# Tools that sign and submit state-changing actions. A timeout on one of
# these must not be read as "the action failed" — see call_tool.
WRITE_TOOLS = frozenset(
    {
        "hyperliquid_place_order",
        "hyperliquid_place_bracket_order",
        "hyperliquid_modify_order",
        "hyperliquid_cancel_order",
        "hyperliquid_cancel_all_orders",
        "hyperliquid_update_leverage",
        "hyperliquid_place_twap_order",
        "hyperliquid_cancel_twap_order",
    }
)

IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9_.:-]{8,100}$")
DEFAULT_STATE_DIR = Path.home() / ".hyperliquid-mcp"


class HyperliquidMCPServer:
    """MCP Server for Hyperliquid trading using the official Python SDK."""

    def __init__(self):
        """Initialize the Hyperliquid MCP server."""
        self.server = Server("hyperliquid-mcp")

        # Load configuration from an optional profile-local TOML file, then env.
        self.config_path = os.getenv("HYPERLIQUID_CONFIG")
        self.config = self._load_config(self.config_path)
        self.private_key = self._config_value("private_key", "HYPERLIQUID_PRIVATE_KEY")
        private_key_file = self._config_value(
            "private_key_file", "HYPERLIQUID_PRIVATE_KEY_FILE"
        )
        if private_key_file:
            self.private_key = self._read_secret_file(private_key_file)
        self.account_address = self._config_value(
            "account_address", "HYPERLIQUID_ACCOUNT_ADDRESS"
        )
        self.vault_address = self._config_value(
            "vault_address", "HYPERLIQUID_VAULT_ADDRESS"
        )
        self.testnet = self._config_bool("testnet", "HYPERLIQUID_TESTNET", False)
        self.trading_enabled = self._config_bool(
            "trading_enabled", "HYPERLIQUID_TRADING_ENABLED", False
        )
        self.allow_main_wallet = self._config_bool(
            "allow_main_wallet", "HYPERLIQUID_ALLOW_MAIN_WALLET", False
        )
        self.require_approval = self._config_bool(
            "require_approval", "HYPERLIQUID_REQUIRE_APPROVAL", False
        )
        self.approval_secret = self._config_value(
            "approval_secret", "HYPERLIQUID_APPROVAL_SECRET", ""
        )
        if self.require_approval and not self.approval_secret:
            raise ValueError(
                "HYPERLIQUID_REQUIRE_APPROVAL=true requires explicit HYPERLIQUID_APPROVAL_SECRET"
            )
        self.state_dir = Path(
            self._config_value(
                "state_dir", "HYPERLIQUID_STATE_DIR", str(DEFAULT_STATE_DIR)
            )
        ).expanduser()
        self.policy = self._load_policy()
        self.health_host = str(
            self._config_value("health_host", "HYPERLIQUID_HEALTH_HOST", "127.0.0.1")
        )
        self.health_port = int(
            self._config_value("health_port", "HYPERLIQUID_HEALTH_PORT", 0) or 0
        )
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

        self._init_ledger()

        # Initialize Hyperliquid SDK
        self._init_hyperliquid()

        # Register handlers
        self._register_handlers()
        self._start_health_server()

    @staticmethod
    def _load_config(path: Optional[str]) -> dict:
        if not path:
            return {}
        if tomllib is None:
            raise RuntimeError("HYPERLIQUID_CONFIG requires Python 3.11+ tomllib")
        p = Path(path).expanduser()
        mode = p.stat().st_mode & 0o777
        if mode & 0o077:
            raise PermissionError(f"Config file {p} must be mode 0600/owner-only")
        with open(p, "rb") as f:
            return tomllib.load(f)

    def _config_value(self, key: str, env: str, default: Any = None) -> Any:
        if env in os.environ:
            return os.environ[env]
        return self.config.get(key, default)

    def _config_bool(self, key: str, env: str, default: bool = False) -> bool:
        raw = self._config_value(key, env, default)
        if isinstance(raw, bool):
            return raw
        return str(raw).lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _read_secret_file(path: str) -> str:
        p = Path(path).expanduser()
        mode = p.stat().st_mode & 0o777
        if mode & 0o077:
            raise PermissionError(f"Secret file {p} must be mode 0600/owner-only")
        return p.read_text().strip()

    def _load_policy(self) -> dict:
        cfg = self.config.get("policy", {}) if isinstance(self.config, dict) else {}

        def val(name: str, env: str, default: Any = None) -> Any:
            return os.getenv(env, cfg.get(name, default))

        allowed_assets_raw = val("allowed_assets", "HYPERLIQUID_ALLOWED_ASSETS", "")
        allowed_assets = {
            a.strip().upper() for a in str(allowed_assets_raw).split(",") if a.strip()
        }
        return {
            "allowed_assets": allowed_assets,
            "max_order_notional_usd": float(
                val("max_order_notional_usd", "HYPERLIQUID_MAX_ORDER_NOTIONAL_USD", 0)
                or 0
            ),
            "max_leverage": int(
                val("max_leverage", "HYPERLIQUID_MAX_LEVERAGE", 0) or 0
            ),
            "max_slippage_bps": float(
                val("max_slippage_bps", "HYPERLIQUID_MAX_SLIPPAGE_BPS", 50) or 50
            ),
            "disable_cancel_all": str(
                val("disable_cancel_all", "HYPERLIQUID_DISABLE_CANCEL_ALL", "true")
            ).lower()
            in {"1", "true", "yes", "on"},
            "require_bracket_for_open": str(
                val(
                    "require_bracket_for_open",
                    "HYPERLIQUID_REQUIRE_BRACKET_FOR_OPEN",
                    "false",
                )
            ).lower()
            in {"1", "true", "yes", "on"},
        }

    def _init_ledger(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.state_dir, 0o700)
        self.ledger_path = self.state_dir / "operations.sqlite"
        with sqlite3.connect(self.ledger_path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""
                CREATE TABLE IF NOT EXISTS operations (
                    idempotency_key TEXT PRIMARY KEY,
                    tool TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    network TEXT NOT NULL,
                    account_address TEXT,
                    created_ms INTEGER NOT NULL,
                    updated_ms INTEGER NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT,
                    readback_json TEXT
                )
                """)
        os.chmod(self.ledger_path, 0o600)

    def _start_health_server(self) -> None:
        """Optional daemon-style health endpoints for external supervisors."""
        if not self.health_port:
            self.health_server = None
            return
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - stdlib callback name
                if self.path not in {"/healthz", "/readyz"}:
                    self.send_response(404)
                    self.end_headers()
                    return
                ready = outer.info is not None and outer.exchange is not None
                self.send_response(200 if self.path == "/healthz" or ready else 503)
                self.send_header("content-type", "application/json")
                self.end_headers()
                payload = {
                    "status": "ok" if ready else "starting",
                    "network": "testnet" if outer.testnet else "mainnet",
                    "trading_enabled": outer.trading_enabled,
                    "ledger": str(outer.ledger_path),
                }
                self.wfile.write(json.dumps(payload, separators=(",", ":")).encode())

            def log_message(self, format, *args):
                return

        self.health_server = ThreadingHTTPServer(
            (self.health_host, self.health_port), Handler
        )
        thread = threading.Thread(target=self.health_server.serve_forever, daemon=True)
        thread.start()
        logger.info(
            "Health endpoints serving on http://%s:%s",
            self.health_host,
            self.health_port,
        )

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
                bootstrap_info = Info(base_url, skip_ws=True, timeout=HTTP_TIMEOUT_SECS)
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
            self.info = Info(
                base_url,
                skip_ws=False,
                perp_dexs=dex_names,
                timeout=HTTP_TIMEOUT_SECS,
            )

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
                timeout=HTTP_TIMEOUT_SECS,
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
            # Info(skip_ws=False) starts NON-daemon WS + ping threads; if we
            # re-raise without stopping them, sys.exit(1) in main() blocks
            # joining them and the "failed" process hangs as a zombie.
            info = getattr(self, "info", None)
            if info is not None:
                try:
                    info.disconnect_websocket()
                except Exception:
                    pass
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
        REST round-trip. But the mirror is only trustworthy while the socket is
        confirmably alive (a message within cold_secs): with a dead socket, the
        deque's residual trades would serve a tape silently missing its most
        recent minutes — worse than an empty one, since CVD/TFI/vwap would be
        computed on it. So ANY read past cold_secs of silence falls back to REST
        (run_forever() does not auto-reconnect), as does cold start (no WS data
        yet — the subscribe->data race). Within cold_secs, an empty window is
        trusted as a real "no flow".
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

        if (time.time() - last_recv) >= cold_secs:
            return self._recent_trades_rest(coin, cutoff_ms), "rest"

        windowed = [t for t in snapshot if t.get("time", 0) >= cutoff_ms]
        return windowed, "websocket"

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
        total_sz = 0.0
        unclassified = 0
        last_px = None
        last_t = None
        min_t = None
        max_t = None
        for t in trades:
            sz = float(t["sz"])
            px = float(t["px"])
            # Only an explicit aggressor tag counts toward signed flow: a
            # missing/unknown side must not silently skew CVD/TFI (the old
            # else-branch counted it as sell volume).
            side = t.get("side")
            if side == "B":
                buy_vol += sz
            elif side == "A":
                sell_vol += sz
            else:
                unclassified += 1
            notional += px * sz
            total_sz += sz
            ts = t.get("time", 0)
            # last_px by max timestamp, not list position: the REST fallback's
            # ordering is not guaranteed chronological.
            if last_t is None or ts >= last_t:
                last_t = ts
                last_px = px
            min_t = ts if min_t is None else min(min_t, ts)
            max_t = ts if max_t is None else max(max_t, ts)

        signed_total = buy_vol + sell_vol
        cvd = buy_vol - sell_vol
        tfi = cvd / signed_total if signed_total else 0.0
        vwap = notional / total_sz if total_sz else last_px
        duration_s = (
            ((max_t - min_t) / 1000.0)
            if (min_t is not None and max_t is not None)
            else 0.0
        )

        out = {
            "buy_vol": round(buy_vol, 6),
            "sell_vol": round(sell_vol, 6),
            "CVD": round(cvd, 6),
            "TFI": round(tfi, 4),
            "trades": len(trades),
            "vwap": round(vwap, 8) if vwap is not None else None,
            "last_px": round(last_px, 8) if last_px is not None else None,
            "duration_s": round(duration_s, 1),
        }
        # Only emitted when nonzero (keeps the dense dict small): trades whose
        # aggressor side wasn't recognized and were excluded from signed flow.
        if unclassified:
            out["unclassified"] = unclassified
        return out

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
            # Sample count behind sigma/m: the candle API caps responses
            # (~5000), so a long lookback at a fine interval silently
            # truncates — this is the caller's only way to detect it.
            "observations": len(arr) - 1,
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
                                "description": "Asset index. ALWAYS resolve via hyperliquid_get_meta first - indices differ between networks (e.g. BTC is 0 on mainnet but 3 on testnet).",
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
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes (8-100 ASCII chars: letters, digits, '.', '_', ':', '-'). Reuse the same key only when retrying the same intent.",
                            },
                        },
                        "required": ["asset", "leverage", "idempotencyKey"],
                    },
                ),
                # Order Management
                Tool(
                    name="hyperliquid_place_order",
                    description="Place a single order on Hyperliquid. Minimum order value is $10. ALWAYS resolve the asset index via hyperliquid_get_meta first - indices differ between networks (e.g. BTC is 0 on mainnet but 3 on testnet).",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "asset": {
                                "type": "integer",
                                "description": "Asset index. ALWAYS resolve via hyperliquid_get_meta first - indices differ between networks (e.g. BTC is 0 on mainnet but 3 on testnet).",
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
                                "description": "Limit price as a string (e.g., '181.5'). Set to '0' for a market-style order: executed as an aggressive IoC limit at mid +/- configured maxSlippageBps protection.",
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
                                "description": "Optional raw Hyperliquid cloid (0x + 32 hex chars). If omitted, the server derives one from idempotencyKey.",
                            },
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes (8-100 ASCII chars). Used to derive/recheck cloid on retry.",
                            },
                            "maxSlippageBps": {
                                "type": "number",
                                "description": "Required/checked for market-style price=0 orders. Must be <= policy max_slippage_bps.",
                            },
                        },
                        "required": ["asset", "isBuy", "size", "idempotencyKey"],
                    },
                ),
                Tool(
                    name="hyperliquid_place_bracket_order",
                    description="Place a complete bracket order (entry + take profit + stop loss) in a single atomic batch. Minimum order value is $10. The TP and SL are reduce-only trigger orders; the TP rests as a limit at its price, the SL triggers as a market order (with configured maxSlippageBps bound) so it cannot gap through unfilled.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "asset": {
                                "type": "integer",
                                "description": "Asset index. ALWAYS resolve via hyperliquid_get_meta first - indices differ between networks (e.g. BTC is 0 on mainnet but 3 on testnet).",
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
                                "description": "Entry limit price as a string (e.g., '181.5'). Set to '0' for market-style entry: executed as an aggressive IoC limit at mid +/- configured maxSlippageBps protection.",
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
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes. The server derives deterministic cloids for entry/tp/sl from it.",
                            },
                            "maxSlippageBps": {
                                "type": "number",
                                "description": "Slippage bound for market-style entry and stop-market limit bound. Must be <= policy max_slippage_bps.",
                            },
                        },
                        "required": [
                            "asset",
                            "isBuy",
                            "size",
                            "takeProfitPrice",
                            "stopLossPrice",
                            "idempotencyKey",
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
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes; reuse only when retrying the same cancel intent.",
                            },
                        },
                        "required": ["coin", "oid", "idempotencyKey"],
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
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes; reuse only when retrying the same cancel-all intent.",
                            },
                        },
                        "required": ["idempotencyKey"],
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
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes; reuse only when retrying the same modify intent.",
                            },
                        },
                        "required": [
                            "oid",
                            "coin",
                            "isBuy",
                            "size",
                            "price",
                            "idempotencyKey",
                        ],
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
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes.",
                            },
                        },
                        "required": [
                            "coin",
                            "isBuy",
                            "size",
                            "minutes",
                            "idempotencyKey",
                        ],
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
                            },
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Required client operation id for signed writes.",
                            },
                        },
                        "required": ["twapId", "idempotencyKey"],
                    },
                ),
                # Order Queries
                Tool(
                    name="hyperliquid_get_operation",
                    description="Read durable operation ledger state by idempotencyKey, including status, payload hash, exchange response, and readback.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "idempotencyKey": {
                                "type": "string",
                                "description": "Operation idempotency key",
                            }
                        },
                        "required": ["idempotencyKey"],
                    },
                ),
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
                payload = {
                    "error": str(e),
                    "tool": name,
                    "arguments": self._redact_arguments(arguments),
                }
                # A read timeout on a write means the signed request may have
                # reached the exchange and executed — "failed" would be a lie.
                if name in WRITE_TOOLS and isinstance(
                    e, requests.exceptions.ReadTimeout
                ):
                    payload["warning"] = (
                        "Request timed out AFTER being sent - the action may"
                        " still have executed. Recheck order status / open"
                        " orders / positions before retrying."
                    )
                    key = arguments.get("idempotencyKey")
                    if key:
                        try:
                            self._update_operation(
                                key, "indeterminate", payload, self._readback_state()
                            )
                        except Exception as ledger_error:
                            payload["ledger_error"] = str(ledger_error)
                elif name in WRITE_TOOLS:
                    key = arguments.get("idempotencyKey")
                    if key:
                        try:
                            self._mark_write_exception_rejected(key, payload)
                        except Exception as ledger_error:
                            payload["ledger_error"] = str(ledger_error)
                return [
                    TextContent(
                        type="text",
                        text=json.dumps(payload, separators=(",", ":")),
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

        if name in WRITE_TOOLS:
            self._guard_write_access(name)
            idempotency_key = self._require_idempotency_key(name, arguments)
            payload_hash, inserted, prior_status = self._record_operation(
                name, arguments, "prepared"
            )
            operation = self._get_operation(idempotency_key).get("operation", {})
            if not inserted and not (
                self.require_approval
                and prior_status == "prepared"
                and arguments.get("approvalToken")
                == self._approval_token(idempotency_key, payload_hash)
            ):
                return {
                    "status": "duplicate_idempotency_key",
                    "message": "Existing durable operation returned; not resubmitting",
                    "operation": operation,
                }
            if self.require_approval:
                expected = self._approval_token(idempotency_key, payload_hash)
                if arguments.get("approvalToken") != expected:
                    return {
                        "status": "awaiting_approval",
                        "message": "Mainnet write prepared but not dispatched; resubmit with approvalToken to execute",
                        "idempotencyKey": idempotency_key,
                        "payloadHash": payload_hash,
                        "approvalToken": expected if self.testnet else None,
                    }
            arguments = self._redact_arguments(arguments)
            if not self._try_transition_operation(
                idempotency_key, {"prepared"}, "submitting", {"tool": name}
            ):
                return {
                    "status": "duplicate_idempotency_key",
                    "message": "Existing durable operation returned; not resubmitting",
                    "operation": self._get_operation(idempotency_key).get(
                        "operation", {}
                    ),
                }
        else:
            idempotency_key = None

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
            assert idempotency_key is not None
            asset = arguments["asset"]  # Already normalized to integer
            leverage = arguments["leverage"]  # Already normalized to integer
            is_cross = arguments.get("isCross", True)

            # Convert asset index to coin name (same guard as place_order)
            coin_name = self.asset_index_to_name.get(asset)
            if coin_name is None:
                raise ValueError(f"Unknown asset index: {asset}")
            self._enforce_asset_policy(coin_name)
            max_lev = int(self.policy.get("max_leverage") or 0)
            if leverage < 1:
                raise ValueError("leverage must be >= 1")
            if max_lev and leverage > max_lev:
                raise PermissionError(
                    f"leverage {leverage} exceeds policy max {max_lev}"
                )

            result = self.exchange.update_leverage(leverage, coin_name, is_cross)

            # Surface a request-level rejection (e.g. leverage above maxLeverage)
            # cleanly instead of returning an opaque {"status": "err"} blob.
            err = self._top_level_error(result)
            readback = self._readback_state(coin_name)
            if err is not None:
                self._update_operation(idempotency_key, "rejected", result, readback)
                return {"error": err, "readback": readback, "requestParams": arguments}

            self._update_operation(idempotency_key, "accepted", result, readback)
            mode = "cross" if is_cross else "isolated"
            return {
                "message": f"Leverage set to {leverage}x ({mode}) for {coin_name}",
                "data": result,
                "readback": readback,
                "summary": {
                    "asset": asset,
                    "coin": coin_name,
                    "leverage": leverage,
                    "marginMode": mode,
                },
            }

        # Order Management
        elif name == "hyperliquid_place_order":
            assert idempotency_key is not None
            asset = arguments["asset"]  # Already normalized to integer
            is_buy = arguments["isBuy"]
            size = self._positive_float("size", arguments["size"])
            # Keep price as string if provided, convert to float for SDK
            price_str = arguments.get("price", "0")
            price = float(price_str) if price_str else 0.0
            if price != 0.0:
                price = self._positive_float("price", price)
            reduce_only = arguments.get("reduceOnly", False)
            if self.policy.get("require_bracket_for_open") and not reduce_only:
                raise PermissionError(
                    "Naked opening place_order blocked by policy: use hyperliquid_place_bracket_order or reduceOnly=true"
                )
            order_type = arguments.get("orderType", {"limit": {"tif": "Gtc"}})
            cloid_str = arguments.get("cloid")

            # Convert asset index to coin name
            coin_name = self.asset_index_to_name.get(asset)
            if coin_name is None:
                raise ValueError(f"Unknown asset index: {asset}")
            self._enforce_asset_policy(coin_name)
            slippage = self._enforce_slippage_policy(
                float(
                    arguments.get("maxSlippageBps") or self.policy["max_slippage_bps"]
                )
            )

            # Create cloid from explicit raw cloid or deterministic idempotency key.
            cloid = (
                Cloid(cloid_str)
                if cloid_str
                else self._cloid_from_idempotency(idempotency_key, "order")
            )

            # Handle trigger orders
            if "trigger" in order_type:
                trigger = order_type["trigger"]
                if "triggerPx" not in trigger:
                    raise ValueError("Trigger orders require a triggerPx")
                trigger["triggerPx"] = self._positive_float(
                    "triggerPx", trigger["triggerPx"]
                )
                if price == 0.0:
                    if trigger.get("isMarket"):
                        # A trigger-market with limit_px 0 could never execute
                        # after triggering (a stop that doesn't stop). Bound it
                        # around the trigger price like the bracket SL: the
                        # slippage-bounded worst acceptable fill.
                        price = self.exchange._slippage_price(
                            coin_name,
                            is_buy,
                            slippage,
                            px=trigger["triggerPx"],
                        )
                    else:
                        raise ValueError(
                            "Non-market trigger orders require an explicit"
                            " positive price (the limit to rest after"
                            " triggering)"
                        )
            elif price == 0.0:
                # Hyperliquid has no native market orders: a resting buy limit
                # at 0 would never fill. Emulate market like the SDK's
                # market_open: aggressive IoC limit at mid +/- 5% slippage.
                price = self.exchange._slippage_price(coin_name, is_buy, slippage)
                order_type = {"limit": {"tif": "Ioc"}}
            self._validate_order_preflight(coin_name, size, price)

            result = self.exchange.order(
                name=coin_name,
                is_buy=is_buy,
                sz=size,
                limit_px=price,
                order_type=order_type,
                reduce_only=reduce_only,
                cloid=cloid,
            )

            # The top-level message must agree with the parsed status — a
            # rejected order under an "Order placed" headline reads as success.
            order_info = self._parse_order_response(result)
            readback = self._readback_state(coin_name, order_info.get("orderId"))
            if order_info["status"] not in {"resting", "filled"}:
                self._update_operation(
                    idempotency_key, "indeterminate", result, readback
                )
                return {
                    "message": f"Order placement not confirmed for {coin_name}",
                    "error": order_info.get("error", "unconfirmed order status"),
                    "data": result,
                    "orderInfo": order_info,
                    "readback": readback,
                    "idempotencyKey": idempotency_key,
                    "cloid": cloid.to_raw(),
                    "requestParams": arguments,
                }

            self._update_operation(
                idempotency_key, order_info["status"], result, readback
            )
            return {
                "message": f"Order placed for {coin_name}",
                "data": result,
                "orderInfo": order_info,
                "readback": readback,
                "idempotencyKey": idempotency_key,
                "cloid": cloid.to_raw(),
                "requestParams": arguments,
            }

        elif name == "hyperliquid_place_bracket_order":
            assert idempotency_key is not None
            asset = arguments["asset"]  # Already normalized to integer
            is_buy = arguments["isBuy"]
            size = self._positive_float("size", arguments["size"])
            # entryPrice 0/omitted means market entry; anything else must be a
            # real price. TP/SL must always be: stopLossPrice 0 would both
            # trigger nonsensically and make _slippage_price silently
            # substitute the mid for the SL's limit bound (its `if not px`
            # fallback).
            entry_price = float(arguments.get("entryPrice", 0) or 0)
            if entry_price != 0.0:
                entry_price = self._positive_float("entryPrice", entry_price)
            tp_price = self._positive_float(
                "takeProfitPrice", arguments["takeProfitPrice"]
            )
            sl_price = self._positive_float("stopLossPrice", arguments["stopLossPrice"])
            reduce_only = arguments.get("reduceOnly", False)
            entry_order_type = arguments.get(
                "entryOrderType", {"limit": {"tif": "Gtc"}}
            )

            # Convert asset index to coin name
            coin_name = self.asset_index_to_name.get(asset)
            if coin_name is None:
                raise ValueError(f"Unknown asset index: {asset}")
            self._enforce_asset_policy(coin_name)
            slippage = self._enforce_slippage_policy(
                float(
                    arguments.get("maxSlippageBps") or self.policy["max_slippage_bps"]
                )
            )

            # Market entry (entryPrice 0/omitted): emulate with an aggressive
            # IoC limit at mid +/- configured slippage — a real limit at 0 would rest
            # forever on the buy side.
            if entry_price == 0.0:
                entry_price = self.exchange._slippage_price(coin_name, is_buy, slippage)
                entry_order_type = {"limit": {"tif": "Ioc"}}

            # Geometry check runs after market-entry resolution so the TP/SL
            # are validated against the price the entry will actually target.
            self._validate_bracket_geometry(is_buy, entry_price, tp_price, sl_price)

            # The stop-loss triggers as MARKET (isMarket True): a limit SL can
            # gap through its price and never fill, defeating the stop. Its
            # limit_px is the slippage-bounded worst fill around the trigger.
            sl_limit_px = self.exchange._slippage_price(
                coin_name, not is_buy, slippage, px=sl_price
            )
            self._validate_order_preflight(coin_name, size, entry_price)

            # Deterministic client IDs make timeout/retry investigation possible.
            entry_cloid = self._cloid_from_idempotency(idempotency_key, "entry")
            tp_cloid = self._cloid_from_idempotency(idempotency_key, "tp")
            sl_cloid = self._cloid_from_idempotency(idempotency_key, "sl")

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
                    "cloid": entry_cloid,
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
                    "cloid": tp_cloid,
                },
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
                    "cloid": sl_cloid,
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
                readback = self._readback_state(coin_name)
                self._update_operation(idempotency_key, "rejected", result, readback)
                return {
                    "message": "Bracket order placement failed",
                    "error": err,
                    "data": result,
                    "readback": readback,
                    "idempotencyKey": idempotency_key,
                    "requestParams": arguments,
                }

            # Parse per-leg statuses: the exchange can return status "ok"
            # with individual legs rejected, and an entry that fills with a
            # rejected SL leg is an unprotected position — that must never
            # read as "placed successfully". A normalTpsl group with an
            # invalid leg is instead rejected atomically (single error
            # status, nothing rests).
            order_infos, failed_legs, group_rejected = self._parse_bracket_result(
                result
            )
            if failed_legs:
                readback = self._readback_state(coin_name)
                self._update_operation(
                    idempotency_key, "indeterminate", result, readback
                )
                if group_rejected:
                    message = (
                        "Bracket order rejected by exchange (atomic group"
                        " reject - no orders were placed)"
                    )
                else:
                    message = "Bracket order partially failed"
                return {
                    "message": message,
                    "error": "; ".join(
                        f"{leg['orderType']}: {leg['error']}" for leg in failed_legs
                    ),
                    "data": result,
                    "orders": order_infos,
                    "readback": readback,
                    "idempotencyKey": idempotency_key,
                    "cloids": {
                        "entry": entry_cloid.to_raw(),
                        "tp": tp_cloid.to_raw(),
                        "sl": sl_cloid.to_raw(),
                    },
                    "requestParams": arguments,
                }

            readback = self._readback_state(coin_name)
            self._update_operation(idempotency_key, "accepted", result, readback)
            return {
                "message": "Bracket order placed successfully",
                "data": result,
                "orders": order_infos,
                "readback": readback,
                "idempotencyKey": idempotency_key,
                "cloids": {
                    "entry": entry_cloid.to_raw(),
                    "tp": tp_cloid.to_raw(),
                    "sl": sl_cloid.to_raw(),
                },
                "requestParams": arguments,
            }

        elif name == "hyperliquid_cancel_order":
            assert idempotency_key is not None
            coin = arguments["coin"]
            oid = arguments["oid"]  # Already normalized to integer
            self._enforce_asset_policy(coin)

            result = self.exchange.cancel(coin, oid)

            summary = self._parse_cancel_result(result, [{"coin": coin, "oid": oid}])
            readback = self._readback_state(coin, oid)
            if summary["failedCount"]:
                self._update_operation(
                    idempotency_key, "indeterminate", result, readback
                )
                return {
                    "message": f"Cancel failed for order {oid} ({coin})",
                    "error": summary.get("error", "unrecognized cancel status"),
                    "data": result,
                    "readback": readback,
                    "requestParams": arguments,
                }

            self._update_operation(idempotency_key, "accepted", result, readback)
            return {
                "message": f"Order {oid} cancelled for {coin}",
                "data": result,
                "readback": readback,
                "cancelledOrder": {"coin": coin, "orderId": oid},
            }

        elif name == "hyperliquid_cancel_all_orders":
            assert idempotency_key is not None
            if self.policy.get("disable_cancel_all"):
                raise PermissionError("cancel_all_orders disabled by policy")
            dex = arguments.get("dex", "")

            # frontend_open_orders (not open_orders) so untriggered TP/SL
            # trigger orders are included — "cancel all" that leaves stale
            # stops resting would fire them later against a flat book.
            open_orders = self.info.frontend_open_orders(user_address, dex=dex)

            if not open_orders:
                self._update_operation(
                    idempotency_key,
                    "accepted",
                    {"status": "ok", "response": {"data": {"statuses": []}}},
                    {},
                )
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
            readback = self._readback_state()
            self._update_operation(
                idempotency_key,
                "accepted" if summary["failedCount"] == 0 else "indeterminate",
                result,
                readback,
            )
            response = {
                "message": (
                    f"Cancelled {summary['cancelledCount']} of "
                    f"{len(cancel_requests)} orders"
                ),
                "data": result,
                "cancelledCount": summary["cancelledCount"],
                "failedCount": summary["failedCount"],
                "outcomes": summary["outcomes"],
                "readback": readback,
            }
            if "error" in summary:
                response["error"] = summary["error"]
            return response

        elif name == "hyperliquid_modify_order":
            assert idempotency_key is not None
            oid = arguments["oid"]  # Already normalized to integer
            coin = arguments["coin"]
            is_buy = arguments["isBuy"]
            size = self._positive_float("size", arguments["size"])
            price = self._positive_float("price", arguments["price"])
            reduce_only = arguments.get("reduceOnly", False)
            order_type = arguments.get("orderType", {"limit": {"tif": "Gtc"}})
            self._enforce_asset_policy(coin)
            self._validate_order_preflight(coin, size, price)

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
            readback = self._readback_state(coin, oid)
            if order_info["status"] not in {"resting", "filled"}:
                self._update_operation(
                    idempotency_key, "indeterminate", result, readback
                )
                return {
                    "message": f"Order {oid} modification not confirmed",
                    "error": order_info.get("error", "unconfirmed modify status"),
                    "data": result,
                    "readback": readback,
                    "requestParams": arguments,
                }

            self._update_operation(
                idempotency_key, order_info["status"], result, readback
            )
            return {
                "message": f"Order {oid} modified successfully",
                "data": result,
                "orderInfo": order_info,
                "readback": readback,
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
        elif name == "hyperliquid_get_operation":
            key = arguments["idempotencyKey"]
            return self._get_operation(key)

        elif name == "hyperliquid_get_open_orders":
            dex = arguments.get("dex", "")
            result = self.info.frontend_open_orders(user_address, dex=dex)

            return {
                "message": "Frontend open orders (regular + trigger/TP/SL) retrieved successfully",
                "data": result,
                "summary": {
                    "numberOfOrders": len(result) if result else 0,
                    "includesTriggers": True,
                },
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
            # Clamp: depth 0/negative would slice nonsense ([:-1] drops the
            # deepest level while claiming the requested depth).
            depth = max(1, arguments.get("depth", 5))

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
            depth = max(1, arguments.get("depth", 5))  # same clamp as order book

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

    def _guard_write_access(self, tool_name: str) -> None:
        """Block real-money writes unless the operator explicitly opts in.

        Testnet remains frictionless so operators can exercise the full path.
        Mainnet writes require HYPERLIQUID_TRADING_ENABLED=true and, by
        default, agent mode (API wallet signing for a distinct main account).
        Using the main wallet key on mainnet is possible only with the explicit
        HYPERLIQUID_ALLOW_MAIN_WALLET=true break-glass flag.
        """
        if self.testnet:
            return
        if not self.trading_enabled:
            raise PermissionError(
                f"{tool_name} blocked on mainnet: set "
                "HYPERLIQUID_TRADING_ENABLED=true to allow signed writes."
            )

        wallet_address = getattr(getattr(self, "wallet", None), "address", None)
        account_address = self.account_address or wallet_address
        if (
            not self.allow_main_wallet
            and wallet_address
            and account_address
            and wallet_address.lower() == account_address.lower()
        ):
            raise PermissionError(
                f"{tool_name} blocked on mainnet: agent mode is required. "
                "Set HYPERLIQUID_ACCOUNT_ADDRESS to the main account and sign "
                "with an approved API wallet, or explicitly set "
                "HYPERLIQUID_ALLOW_MAIN_WALLET=true."
            )

    @staticmethod
    def _require_idempotency_key(tool_name: str, arguments: dict) -> str:
        """Require an operator-supplied idempotency key for every write."""
        key = arguments.get("idempotencyKey")
        if not isinstance(key, str) or not key:
            raise ValueError(f"{tool_name} requires idempotencyKey for signed writes")
        if not IDEMPOTENCY_KEY_RE.fullmatch(key):
            try:
                key.encode("ascii")
            except UnicodeEncodeError:
                raise ValueError(
                    "idempotencyKey must contain only ASCII letters, digits, '.', '_', ':', or '-'"
                )
            raise ValueError(
                "idempotencyKey must be 8-100 characters using letters, digits, '.', '_', ':', or '-'"
            )
        return key

    @staticmethod
    def _payload_hash(tool_name: str, arguments: dict) -> str:
        clean = HyperliquidMCPServer._redact_arguments(arguments)
        raw = json.dumps({"tool": tool_name, "arguments": clean}, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _redact_arguments(arguments: dict) -> dict:
        return {k: v for k, v in arguments.items() if k != "approvalToken"}

    def _approval_token(self, idempotency_key: str, payload_hash: str) -> str:
        if not self.approval_secret:
            raise ValueError("approval_secret is required to compute approval tokens")
        secret = self.approval_secret
        return hmac.new(
            secret.encode("utf-8"),
            f"{idempotency_key}:{payload_hash}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()[:16]

    def _record_operation(
        self, tool_name: str, arguments: dict, status: str
    ) -> tuple[str, bool, Optional[str]]:
        idempotency_key = arguments["idempotencyKey"]
        payload_hash = self._payload_hash(tool_name, arguments)
        now = int(time.time() * 1000)
        network = "testnet" if self.testnet else "mainnet"
        request_json = json.dumps(
            self._redact_arguments(arguments), sort_keys=True, default=str
        )
        with sqlite3.connect(self.ledger_path) as db:
            db.execute("BEGIN IMMEDIATE")
            cur = db.execute(
                """
                INSERT OR IGNORE INTO operations
                (idempotency_key,tool,payload_hash,status,network,account_address,created_ms,updated_ms,request_json)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    idempotency_key,
                    tool_name,
                    payload_hash,
                    status,
                    network,
                    self.account_address,
                    now,
                    now,
                    request_json,
                ),
            )
            inserted = cur.rowcount == 1
            row = db.execute(
                "SELECT payload_hash,status FROM operations WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if row is None:
                raise RuntimeError("operation ledger insert/read failed")
            if row[0] != payload_hash:
                raise ValueError(
                    "idempotencyKey already exists with a different payload; use a new key"
                )
            return payload_hash, inserted, row[1]

    def _get_operation(self, idempotency_key: str) -> dict:
        with sqlite3.connect(self.ledger_path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute(
                "SELECT * FROM operations WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
        if row is None:
            return {"status": "not_found", "idempotencyKey": idempotency_key}
        out = dict(row)
        for field in ("request_json", "response_json", "readback_json"):
            if out.get(field):
                try:
                    out[field[:-5]] = json.loads(out[field])
                except Exception:
                    out[field[:-5]] = out[field]
                del out[field]
        return {"status": "ok", "operation": out}

    def _update_operation(
        self, idempotency_key: str, status: str, response: Any, readback: Any = None
    ) -> None:
        with sqlite3.connect(self.ledger_path) as db:
            db.execute(
                """
                UPDATE operations
                SET status=?,updated_ms=?,response_json=?,readback_json=?
                WHERE idempotency_key=?
                """,
                (
                    status,
                    int(time.time() * 1000),
                    json.dumps(response, sort_keys=True, default=str),
                    (
                        json.dumps(readback, sort_keys=True, default=str)
                        if readback
                        else None
                    ),
                    idempotency_key,
                ),
            )

    def _try_transition_operation(
        self,
        idempotency_key: str,
        from_statuses: set[str],
        to_status: str,
        response: Any,
        readback: Any = None,
    ) -> bool:
        with sqlite3.connect(self.ledger_path) as db:
            cur = db.execute(
                """
                UPDATE operations
                SET status=?,updated_ms=?,response_json=?,readback_json=?
                WHERE idempotency_key=? AND status IN (%s)
                """ % ",".join("?" for _ in from_statuses),
                (
                    to_status,
                    int(time.time() * 1000),
                    json.dumps(response, sort_keys=True, default=str),
                    (
                        json.dumps(readback, sort_keys=True, default=str)
                        if readback
                        else None
                    ),
                    idempotency_key,
                    *from_statuses,
                ),
            )
            return cur.rowcount == 1

    def _mark_write_exception_rejected(
        self, idempotency_key: str, payload: dict
    ) -> None:
        if "different payload" in str(payload.get("error", "")):
            return
        existing = self._get_operation(idempotency_key)
        op = existing.get("operation") or {}
        # Do not let a bad retry (same key, different payload) corrupt a prior
        # durable operation row. Only pre-dispatch failures can be rejected.
        # Once a request is submitting, a generic exception may mean the signed
        # request reached the exchange and then parsing/ledger/readback failed.
        if op.get("status") == "prepared":
            self._update_operation(
                idempotency_key,
                "rejected",
                self._redact_arguments(payload),
                None,
            )
        elif op.get("status") == "submitting":
            self._update_operation(
                idempotency_key,
                "indeterminate",
                self._redact_arguments(payload),
                self._readback_state(),
            )

    def _readback_state(
        self, coin: Optional[str] = None, oid: Optional[int] = None
    ) -> dict:
        """Best-effort post-write readback used before reporting final state."""
        out: dict[str, Any] = {}
        read_address = self.vault_address or self.account_address
        try:
            out["positions"] = self.info.user_state(read_address)
        except Exception as e:
            out["positions_error"] = str(e)
        try:
            out["openOrders"] = self.info.frontend_open_orders(read_address)
        except Exception as e:
            out["openOrders_error"] = str(e)
        if oid is not None:
            try:
                out["orderStatus"] = self.info.query_order_by_oid(read_address, oid)
            except Exception as e:
                out["orderStatus_error"] = str(e)
        if coin:
            out["coin"] = coin
        return out

    def _enforce_asset_policy(self, coin: str) -> None:
        allowed = self.policy.get("allowed_assets") or set()
        base_coin = coin.split(":")[-1].upper()
        if allowed and base_coin not in allowed and coin.upper() not in allowed:
            raise PermissionError(f"Asset {coin} is not in HYPERLIQUID_ALLOWED_ASSETS")

    def _enforce_slippage_policy(self, bps: float) -> float:
        max_bps = float(self.policy.get("max_slippage_bps") or 0)
        if max_bps and bps > max_bps:
            raise PermissionError(f"maxSlippageBps {bps} exceeds policy max {max_bps}")
        if bps <= 0:
            raise ValueError("maxSlippageBps must be positive")
        return bps / 10000.0

    def _validate_order_preflight(self, coin: str, size: float, price: float) -> None:
        try:
            meta = self.info.meta()
            asset_idx = self.info.name_to_asset.get(coin)
            if asset_idx is not None and asset_idx < len(meta.get("universe", [])):
                info = meta["universe"][asset_idx]
                sz_decimals = int(info.get("szDecimals", 8))
                size_text = f"{size:.16f}".rstrip("0").rstrip(".")
                decimals = len(size_text.split(".")[1]) if "." in size_text else 0
                if decimals > sz_decimals:
                    raise ValueError(
                        f"size exceeds szDecimals={sz_decimals} for {coin}"
                    )
        except ValueError:
            raise
        except Exception as e:
            logger.warning(f"Preflight precision validation skipped for {coin}: {e}")
        if price > 0 and size * price < 10:
            raise ValueError("Order notional must be at least $10")
        max_notional = float(self.policy.get("max_order_notional_usd") or 0)
        if max_notional and price > 0 and size * price > max_notional:
            raise PermissionError(f"Order notional exceeds policy max ${max_notional}")

    @staticmethod
    def _cloid_from_idempotency(idempotency_key: str, suffix: str) -> Cloid:
        """Derive a valid 16-byte Hyperliquid cloid from an idempotency key."""
        digest = hashlib.blake2s(
            f"{idempotency_key}:{suffix}".encode("utf-8"), digest_size=16
        ).hexdigest()
        return Cloid(f"0x{digest}")

    @staticmethod
    def _validate_bracket_geometry(
        is_buy: bool, entry_price: float, tp_price: float, sl_price: float
    ) -> None:
        """Reject a bracket whose TP/SL sit on the wrong side of the entry.

        A wrong-side stop triggers the instant it hits the book — a
        reduce-only market close at up to 5% slippage — so this must fail
        before anything is signed. Longs need SL < entry < TP; shorts the
        mirror.
        """
        if is_buy:
            if not (sl_price < entry_price < tp_price):
                raise ValueError(
                    f"Invalid bracket for long: require stopLossPrice < entryPrice"
                    f" < takeProfitPrice, got SL {sl_price}, entry {entry_price},"
                    f" TP {tp_price}"
                )
        else:
            if not (tp_price < entry_price < sl_price):
                raise ValueError(
                    f"Invalid bracket for short: require takeProfitPrice <"
                    f" entryPrice < stopLossPrice, got TP {tp_price}, entry"
                    f" {entry_price}, SL {sl_price}"
                )

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
        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
        order_status = statuses[0] if statuses else None
        parsed = self._parse_order_status(order_status)
        if parsed["status"] == "unknown":
            return self._indeterminate_write_status(order_status)
        return parsed

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
        """Parse bulk_orders bracket statuses into
        (order_infos, failed_legs, group_rejected).

        Statuses map positionally to the submitted order list (entry first) —
        but only when the exchange returns one status per order. A normalTpsl
        group with an invalid leg is rejected ATOMICALLY (verified on
        testnet): the response carries a SINGLE error status for the whole
        group and nothing rests, so positional leg attribution would be wrong
        (the error may describe any leg). group_rejected=True flags that case.
        Per-leg inspection still matters for the one-status-per-order shape —
        an entry that fills while its SL leg was rejected is an unprotected
        position.
        """
        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
        leg_names = ["entry", "take-profit", "stop-loss"]
        group_rejected = 0 < len(statuses) < len(leg_names) and any(
            isinstance(s, dict) and "error" in s for s in statuses
        )
        order_infos = []
        for idx, status in enumerate(statuses):
            info = self._parse_order_status(status)
            if group_rejected:
                info["orderType"] = "group"
            else:
                info["orderType"] = (
                    leg_names[idx] if idx < len(leg_names) else f"leg-{idx}"
                )
            order_infos.append(info)
        failed_legs = [
            i for i in order_infos if i["status"] in {"error", "indeterminate"}
        ]
        return order_infos, failed_legs, group_rejected

    @staticmethod
    def _indeterminate_write_status(raw_status: Any) -> dict:
        return {
            "status": "indeterminate",
            "error": "Exchange response did not contain a known success or rejection status",
            "message": "Write outcome is indeterminate; recheck by cloid/order status/open orders/fills before retrying",
            "mayHaveExecuted": True,
            "rawStatus": raw_status,
        }

    def _parse_order_status(self, status: Any) -> dict:
        """Parse a single order status.

        Statuses are usually dicts keyed by outcome, but the exchange also
        uses bare strings: normalTpsl TP/SL children come back as
        "waitingForFill" (accepted, activates when the entry fills).
        """
        if status == "waitingForFill":
            return {
                "status": "waitingForFill",
                "message": "Trigger order accepted; activates when the entry fills",
            }
        if not isinstance(status, dict):
            return self._indeterminate_write_status(status)
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
            return self._indeterminate_write_status(status)

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
