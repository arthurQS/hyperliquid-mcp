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
# _indicators / _ema / _rsi
# ---------------------------------------------------------------------------


def _ref_ema(arr, period):
    """Independent reference EMA (SMA seed + k=2/(period+1)) for cross-check."""
    k = 2.0 / (period + 1)
    ema = float(np.mean(arr[:period]))
    for x in arr[period:]:
        ema = float(x) * k + ema * (1 - k)
    return ema


class TestEMA:
    def test_matches_reference(self):
        arr = np.asarray([float(x) for x in range(1, 61)], dtype=float)
        for p in (9, 21, 50):
            assert math.isclose(S._ema(arr, p), _ref_ema(arr, p), rel_tol=1e-12)

    def test_insufficient_returns_none(self):
        assert S._ema(np.arange(5, dtype=float), 9) is None


class TestRSI:
    def test_rising_series_maxes_out(self):
        closes = np.asarray([100.0 + i for i in range(30)], dtype=float)
        assert S._rsi(closes, 14) == 100.0  # only gains

    def test_falling_series_bottoms_out(self):
        closes = np.asarray([100.0 - i for i in range(30)], dtype=float)
        assert S._rsi(closes, 14) == 0.0  # only losses

    def test_range_and_seeding(self):
        rng = np.random.default_rng(9)
        closes = 100.0 + np.cumsum(rng.standard_normal(100))
        val = S._rsi(closes, 14)
        assert 0.0 <= val <= 100.0

    def test_insufficient_returns_none(self):
        assert S._rsi(np.arange(10, dtype=float), 14) is None


class TestIndicators:
    def _rising_closes(self, n=260):
        return [str(100.0 + i) for i in range(n)]

    def test_rising_series_flags(self):
        closes = self._rising_closes()
        vols = ["10"] * len(closes)
        out = S._indicators(closes, vols)
        # Strong uptrend: RSI overbought, price above every EMA, EMAs stacked up.
        assert out["rsi"]["is_overbought"] is True
        assert out["rsi"]["zone"] == "OVERBOUGHT"
        for p in ("9", "21", "50", "200"):
            assert out["ema"][p]["price_is_above"] is True
        assert out["ema"]["is_ordered_up"] is True
        assert out["ema"]["is_ordered_down"] is False
        assert out["price"] == float(closes[-1])

    def test_ema200_null_when_short_history_but_tool_returns(self):
        closes = [str(100.0 + i) for i in range(60)]  # < 200
        vols = ["1"] * len(closes)
        out = S._indicators(closes, vols)
        assert out is not None
        assert out["ema"]["200"]["value"] is None
        assert out["ema"]["9"]["value"] is not None
        # ordering undefined when any EMA is missing
        assert out["ema"]["is_ordered_up"] is None

    def test_bollinger_percent_b_and_bands(self):
        # Constant series -> zero-width bands (bandwidth 0, percent_b undefined).
        flat = ["50"] * 30
        out = S._indicators(flat, ["1"] * 30, rsi_period=14)
        assert out["bollinger"]["bandwidth"] == 0.0
        assert out["bollinger"]["percent_b"] is None
        assert out["bollinger"]["price_vs_bands"] == "INSIDE"

    def test_bollinger_price_above_upper(self):
        closes = ["10"] * 19 + ["100"]  # last close spikes above the band
        out = S._indicators(closes, ["1"] * 20, rsi_period=14, bb_period=20)
        assert out["bollinger"]["price_vs_bands"] == "ABOVE_UPPER"
        assert out["bollinger"]["percent_b"] > 1.0

    def test_volume_sma(self):
        closes = self._rising_closes(30)
        vols = ["10"] * 29 + ["30"]  # last volume is 3x the baseline
        out = S._indicators(closes, vols, vol_sma_period=20)
        assert out["volume"]["current"] == 30.0
        assert out["volume"]["above_sma"] is True
        assert out["volume"]["ratio"] > 1.0

    def test_insufficient_returns_none(self):
        assert S._indicators(["100", "101"], ["1", "1"], rsi_period=14) is None

    def test_non_positive_close_returns_none(self):
        closes = ["100"] * 14 + ["-1"]
        assert S._indicators(closes, ["1"] * 15, rsi_period=14) is None


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

    def test_unknown_fails_closed(self):
        out = self.parse({"weird": 1})
        assert out["status"] == "indeterminate"
        assert out["mayHaveExecuted"] is True


# ---------------------------------------------------------------------------
# _align_candles
# ---------------------------------------------------------------------------


def _candle(t, c):
    return {"t": t, "c": str(c)}


class TestAlignCandles:
    def test_intersects_and_sorts(self):
        a = [_candle(3, 30), _candle(1, 10), _candle(2, 20)]  # out of order
        b = [_candle(2, 200), _candle(3, 300), _candle(9, 900)]  # 9 unshared
        ac, bc = S._align_candles(a, b)
        assert ac == ["20", "30"]  # sorted by t, only shared 2 & 3
        assert bc == ["200", "300"]

    def test_disjoint_returns_empty(self):
        ac, bc = S._align_candles([_candle(1, 10)], [_candle(2, 20)])
        assert ac == [] and bc == []

    def test_handles_empty_input(self):
        assert S._align_candles(None, None) == ([], [])
        assert S._align_candles([], [_candle(1, 1)]) == ([], [])


# ---------------------------------------------------------------------------
# _beta
# ---------------------------------------------------------------------------


def bench_closes(n=300, sigma=0.02, s0=100.0, seed=11):
    rng = np.random.default_rng(seed)
    logret = sigma * rng.standard_normal(n)
    return list(s0 * np.exp(np.cumsum(logret))), logret


class TestBeta:
    def test_self_beta_is_one(self):
        closes, _ = bench_closes()
        out = S._beta(closes, closes)
        assert out["beta"] == pytest.approx(1.0, abs=1e-9)
        assert out["correlation"] == pytest.approx(1.0, abs=1e-9)
        assert out["r_squared"] == pytest.approx(1.0, abs=1e-9)
        assert out["observations"] == len(closes) - 1

    def test_scaled_series_beta_two(self):
        # Asset log returns are exactly 2x the benchmark's -> beta == 2, rho == 1.
        b, rb = bench_closes()
        a = list(100.0 * np.exp(np.cumsum(2.0 * rb)))
        out = S._beta(a, b)
        assert out["beta"] == pytest.approx(2.0, abs=1e-9)
        assert out["correlation"] == pytest.approx(1.0, abs=1e-9)

    def test_anticorrelated_beta_negative(self):
        b, rb = bench_closes()
        a = list(100.0 * np.exp(np.cumsum(-rb)))  # exact mirror
        out = S._beta(a, b)
        assert out["beta"] == pytest.approx(-1.0, abs=1e-9)
        assert out["correlation"] == pytest.approx(-1.0, abs=1e-9)
        assert out["r_squared"] == pytest.approx(1.0, abs=1e-9)

    def test_flat_asset_beta_zero_corr_none(self):
        b, _ = bench_closes()
        a = [100.0] * len(b)  # zero variance
        out = S._beta(a, b)
        assert out["beta"] == 0.0
        assert out["correlation"] is None and out["r_squared"] is None

    def test_bad_inputs_return_none(self):
        b, _ = bench_closes(n=10)
        assert S._beta([100, 101], [100, 101]) is None  # <3 closes
        assert S._beta(b, b[:-1]) is None  # mismatched lengths
        assert S._beta([100, 100, 100], [100, 100, 100]) is None  # zero bench var
        assert S._beta([100, -1, 102], [100, 101, 102]) is None  # log(<=0)

    def test_returns_are_json_safe(self):
        import json

        closes, _ = bench_closes()
        out = S._beta(closes, closes)
        json.dumps(out)  # must not raise on np.float64/np.bool_


# ---------------------------------------------------------------------------
# Convention-pinning golden values (computed with independent references)
# ---------------------------------------------------------------------------

# Deterministic 40-close series (seeded random walk, values frozen here).
GOLDEN_CLOSES = [
    100.3047,
    99.2647,
    100.0152,
    100.9557,
    99.0047,
    97.7025,
    97.8304,
    97.5141,
    97.4973,
    96.6443,
    97.5237,
    98.3015,
    98.3675,
    99.4947,
    99.9623,
    99.103,
    99.4717,
    98.5128,
    99.3913,
    99.3414,
    99.1565,
    98.4756,
    99.6981,
    99.5436,
    99.1153,
    98.7631,
    99.2954,
    99.6609,
    100.0736,
    100.5044,
    102.6461,
    102.2397,
    101.7274,
    100.9136,
    101.5296,
    102.6586,
    102.5446,
    101.7045,
    100.88,
    101.5306,
]


class TestGoldenConventions:
    def test_rsi_is_wilder_not_cutler(self):
        # Wilder RMA gives 54.1270 on this series; Cutler's SMA-RSI gives
        # 64.1348 — a formula swap fails loudly here.
        val = S._rsi(np.asarray(GOLDEN_CLOSES), 14)
        assert val == pytest.approx(54.12701977595044, abs=1e-9)

    def test_bollinger_uses_population_std(self):
        # ddof=0 upper band is 103.32547; ddof=1 would give 103.39541.
        out = S._indicators(
            [str(c) for c in GOLDEN_CLOSES],
            ["1"] * len(GOLDEN_CLOSES),
            bb_period=20,
            bb_stddev=2.0,
        )
        assert out["bollinger"]["upper"] == pytest.approx(103.32546739, abs=1e-6)
        assert out["bollinger"]["lower"] == pytest.approx(97.94065261, abs=1e-6)

    def test_ema_golden_value(self):
        # SMA-seeded EMA with k=2/(p+1), independently computed.
        val = S._ema(np.asarray(GOLDEN_CLOSES), 9)
        assert val == pytest.approx(101.47018887413131, abs=1e-9)


class TestOrderflowEdges:
    def test_last_px_is_max_time_not_list_order(self):
        # REST fallback ordering is not guaranteed chronological: newest-first
        # input must still report the newest trade's price as last_px.
        trades = [
            _trade(102, 1, "B", 6_000),  # newest, listed first
            _trade(101, 1, "A", 3_000),
            _trade(100, 2, "B", 1_000),  # oldest, listed last
        ]
        out = S._orderflow(trades, window_secs=60)
        assert out["last_px"] == 102.0

    def test_unknown_side_not_counted_as_sell(self):
        trades = [
            _trade(100, 2, "B", 1_000),
            {"px": "101", "sz": "5", "time": 2_000},  # no side tag
        ]
        out = S._orderflow(trades, window_secs=60)
        assert out["sell_vol"] == 0.0  # must not absorb the untagged 5.0
        assert out["buy_vol"] == 2.0
        assert out["TFI"] == 1.0
        assert out["unclassified"] == 1
        # vwap still includes the untagged trade's real notional
        assert out["vwap"] == round((100 * 2 + 101 * 5) / 7, 8)


class TestMonteCarloObservations:
    def test_reports_sample_count(self):
        closes = gbm_closes(n=500)
        out = S._monte_carlo(
            closes, 100.0, 24, 1000, False, rng=np.random.default_rng(8)
        )
        # The only way a caller can detect a truncated candle fetch.
        assert out["observations"] == len(closes) - 1
