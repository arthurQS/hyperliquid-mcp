"""Unit tests for the pure computation helpers in HyperliquidMCPServer.

These are staticmethods (plus one self-free instance method), so no server
instance, network, or environment variables are needed.
"""

import math

import numpy as np
import pytest

from hyperliquid_mcp.server import HyperliquidMCPServer as S

# ---------------------------------------------------------------------------
# _microstructure
# ---------------------------------------------------------------------------


def _lvl(px, sz):
    return {"px": str(px), "sz": str(sz), "n": 1}


class TestMicrostructure:
    def test_known_book(self):
        levels = [[_lvl(99, 2)], [_lvl(101, 1)]]
        out = S._microstructure(levels, depth=5)
        assert out["mid"] == 100.0
        assert out["spread_bps"] == 200.0
        # OBI = bid volume share = 2 / 3
        assert out["OBI"] == round(2 / 3, 4)
        # Stoikov: (bid*ask_sz + ask*bid_sz) / (bid_sz + ask_sz)
        assert out["micro_price"] == round((99 * 1 + 101 * 2) / 3, 8)

    def test_depth_truncation(self):
        bids = [_lvl(100, 1), _lvl(99, 100)]
        asks = [_lvl(101, 1), _lvl(102, 100)]
        out = S._microstructure([bids, asks], depth=1)
        assert out["OBI"] == 0.5  # deep levels excluded

    def test_empty_side_returns_none(self):
        assert S._microstructure([[], [_lvl(101, 1)]], depth=5) is None
        assert S._microstructure([[_lvl(99, 1)], []], depth=5) is None


# ---------------------------------------------------------------------------
# _orderflow
# ---------------------------------------------------------------------------


def _trade(px, sz, side, t):
    return {"px": str(px), "sz": str(sz), "side": side, "time": t}


class TestOrderflow:
    def test_known_tape(self):
        trades = [
            _trade(100, 2, "B", 1_000),
            _trade(101, 1, "A", 3_000),
            _trade(102, 1, "B", 6_000),
        ]
        out = S._orderflow(trades, window_secs=60)
        assert out["buy_vol"] == 3.0
        assert out["sell_vol"] == 1.0
        assert out["CVD"] == 2.0
        assert out["TFI"] == 0.5  # (3-1)/4
        assert out["trades"] == 3
        assert out["vwap"] == round((100 * 2 + 101 * 1 + 102 * 1) / 4, 8)
        assert out["last_px"] == 102.0
        assert out["duration_s"] == 5.0

    def test_empty_returns_none(self):
        assert S._orderflow([], window_secs=60) is None


# ---------------------------------------------------------------------------
# _monte_carlo
# ---------------------------------------------------------------------------


def gbm_closes(n=500, mu=0.001, sigma=0.02, s0=100.0, seed=7):
    rng = np.random.default_rng(seed)
    logret = mu + sigma * rng.standard_normal(n)
    return list(s0 * np.exp(np.cumsum(logret)))


class TestMonteCarlo:
    def test_deterministic_with_seeded_rng(self):
        closes = gbm_closes()
        a = S._monte_carlo(
            closes, 100.0, 24, 10_000, False, rng=np.random.default_rng(1)
        )
        b = S._monte_carlo(
            closes, 100.0, 24, 10_000, False, rng=np.random.default_rng(1)
        )
        assert a == b

    def test_zero_drift_is_martingale(self):
        # "Zero drift" = zero expected *return*: E[S_T] == s0.
        closes = gbm_closes(mu=0.005)  # strong historical drift, must be ignored
        out = S._monte_carlo(
            closes, 100.0, 168, 200_000, False, rng=np.random.default_rng(2)
        )
        assert abs(out["expected_return"]) < 0.01
        assert out["mu_per_step"] < 0  # -sigma^2/2

    def test_historical_drift_no_double_ito(self):
        # With historical drift, the per-step log-return mean must be used
        # as-is (it already embeds -sigma^2/2). The median terminal of a
        # lognormal is s0*exp(steps*m), which pins m exactly.
        closes = gbm_closes(mu=0.002, sigma=0.01, seed=11)
        m = float(np.diff(np.log(closes)).mean())
        steps = 100
        out = S._monte_carlo(
            closes, 100.0, steps, 200_000, True, rng=np.random.default_rng(3)
        )
        assert out["mu_per_step"] == round(m, 8)
        expected_median = 100.0 * math.exp(steps * m)
        assert out["median_terminal"] == pytest.approx(expected_median, rel=0.01)

    def test_sigma_estimate(self):
        closes = gbm_closes(mu=0.0, sigma=0.02, n=5000, seed=5)
        out = S._monte_carlo(
            closes, 100.0, 24, 1000, False, rng=np.random.default_rng(4)
        )
        assert out["sigma_per_step"] == pytest.approx(0.02, rel=0.05)

    def test_var_is_positive_loss_magnitude(self):
        out = S._monte_carlo(
            gbm_closes(), 100.0, 168, 50_000, False, rng=np.random.default_rng(6)
        )
        assert out["VaR_5pct"] > 0
        assert out["p05_terminal"] < out["median_terminal"] < out["p95_terminal"]
        assert 0.0 <= out["prob_profit"] <= 1.0

    def test_bad_inputs_return_none(self):
        assert S._monte_carlo([100, 101], 100.0, 10, 100, False) is None  # <3 closes
        assert S._monte_carlo([100, 100, 100], 100.0, 10, 100, False) is None  # 0 vol
        assert S._monte_carlo([100, -1, 102], 100.0, 10, 100, False) is None  # log(<=0)
        assert S._monte_carlo(gbm_closes(), 0.0, 10, 100, False) is None  # bad s0


# ---------------------------------------------------------------------------
# _parse_order_status
# ---------------------------------------------------------------------------


class TestParseOrderStatus:
    def parse(self, status):
        return S._parse_order_status(object.__new__(S), status)

    def test_resting(self):
        out = self.parse({"resting": {"oid": 42}})
        assert out["status"] == "resting" and out["orderId"] == 42

    def test_filled(self):
        out = self.parse({"filled": {"oid": 7, "totalSz": "1.5", "avgPx": "100.2"}})
        assert out["status"] == "filled"
        assert out["totalSize"] == "1.5" and out["averagePrice"] == "100.2"

    def test_error(self):
        out = self.parse({"error": "Insufficient margin"})
        assert out["status"] == "error" and out["error"] == "Insufficient margin"

    def test_unknown(self):
        assert self.parse({"weird": 1})["status"] == "unknown"
