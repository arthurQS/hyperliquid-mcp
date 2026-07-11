"""Write-path fixture tests: response parsing and input validation.

These pin the truth-telling contract of the write tools — a rejected
exchange action must never be reported as success — and the input guards
that reject garbage before anything reaches signing. All helpers under
test are pure (staticmethods or self-free instance methods), so no
server instance, network, or environment is needed.
"""

import pytest

from hyperliquid_mcp.server import HyperliquidMCPServer as S


def _inst():
    """Bare instance for self-taking methods that never touch self state."""
    return object.__new__(S)


# ---------------------------------------------------------------------------
# _top_level_error
# ---------------------------------------------------------------------------


class TestTopLevelError:
    def test_ok_response_is_none(self):
        ok = {"status": "ok", "response": {"data": {"statuses": []}}}
        assert S._top_level_error(ok) is None

    def test_err_with_string_response(self):
        err = {"status": "err", "response": "User or API Wallet does not exist."}
        assert S._top_level_error(err) == "User or API Wallet does not exist."

    def test_err_with_dict_response_stringified(self):
        err = {"status": "err", "response": {"code": 42}}
        assert S._top_level_error(err) == str({"code": 42})

    def test_non_dict_result_stringified(self):
        assert S._top_level_error("boom") == "boom"
        assert S._top_level_error(None) == "None"


# ---------------------------------------------------------------------------
# _parse_order_response
# ---------------------------------------------------------------------------


def _order_result(status):
    return {"status": "ok", "response": {"data": {"statuses": [status]}}}


class TestParseOrderResponse:
    def test_resting(self):
        out = _inst()._parse_order_response(_order_result({"resting": {"oid": 7}}))
        assert out["status"] == "resting" and out["orderId"] == 7

    def test_filled(self):
        out = _inst()._parse_order_response(
            _order_result({"filled": {"oid": 7, "totalSz": "1.0", "avgPx": "99.5"}})
        )
        assert out["status"] == "filled"

    def test_per_status_error(self):
        out = _inst()._parse_order_response(
            _order_result({"error": "Price must be divisible by tick size."})
        )
        assert out["status"] == "error"
        assert "tick size" in out["error"]

    def test_top_level_err(self):
        out = _inst()._parse_order_response(
            {"status": "err", "response": "Insufficient margin"}
        )
        assert out["status"] == "error"
        assert out["error"] == "Insufficient margin"

    def test_empty_statuses_is_unknown(self):
        out = _inst()._parse_order_response(
            {"status": "ok", "response": {"data": {"statuses": [{}]}}}
        )
        assert out["status"] == "unknown"


# ---------------------------------------------------------------------------
# _positive_float
# ---------------------------------------------------------------------------


class TestPositiveFloat:
    def test_accepts_positive_string_and_number(self):
        assert S._positive_float("size", "0.5") == 0.5
        assert S._positive_float("price", 100) == 100.0

    @pytest.mark.parametrize("bad", ["NaN", "nan", "inf", "-inf", "Infinity"])
    def test_rejects_non_finite(self, bad):
        with pytest.raises(ValueError, match="finite positive"):
            S._positive_float("size", bad)

    @pytest.mark.parametrize("bad", ["-1", "0", 0, -0.5])
    def test_rejects_non_positive(self, bad):
        with pytest.raises(ValueError, match="finite positive"):
            S._positive_float("size", bad)

    @pytest.mark.parametrize("bad", ["abc", None, {}, []])
    def test_rejects_non_numeric(self, bad):
        with pytest.raises(ValueError, match="Must be a number"):
            S._positive_float("size", bad)


# ---------------------------------------------------------------------------
# _coerce_int
# ---------------------------------------------------------------------------


class TestCoerceInt:
    def test_accepts_int_intlike_float_and_string(self):
        assert S._coerce_int("asset", 5) == 5
        assert S._coerce_int("asset", 5.0) == 5
        assert S._coerce_int("oid", "123") == 123
        assert S._coerce_int("oid", "123.0") == 123

    @pytest.mark.parametrize("bad", [5.7, "123.9", -1.5])
    def test_rejects_fractional(self, bad):
        with pytest.raises(ValueError, match="whole number"):
            S._coerce_int("asset", bad)

    @pytest.mark.parametrize("bad", ["nan", "inf"])
    def test_rejects_non_finite(self, bad):
        with pytest.raises(ValueError, match="whole number"):
            S._coerce_int("asset", bad)

    @pytest.mark.parametrize("bad", ["abc", None, {}])
    def test_rejects_non_numeric(self, bad):
        with pytest.raises(ValueError, match="valid integer"):
            S._coerce_int("asset", bad)

    def test_negative_integers_pass_through(self):
        # Range checks (e.g. oid existence) are the exchange's job; this
        # helper only guards integrality.
        assert S._coerce_int("startTime", -1) == -1


# ---------------------------------------------------------------------------
# _parse_cancel_result
# ---------------------------------------------------------------------------


def _cancel_result(statuses):
    return {
        "status": "ok",
        "response": {"type": "cancel", "data": {"statuses": statuses}},
    }


class TestParseCancelResult:
    def test_all_success(self):
        req = [{"coin": "BTC", "oid": 1}, {"coin": "ETH", "oid": 2}]
        out = _inst()._parse_cancel_result(_cancel_result(["success", "success"]), req)
        assert out["cancelledCount"] == 2 and out["failedCount"] == 0
        assert "error" not in out
        assert all(o["status"] == "success" for o in out["outcomes"])

    def test_per_status_error_is_failed(self):
        req = [{"coin": "BTC", "oid": 1}]
        never = {"error": "Order was never placed, already canceled, or filled."}
        out = _inst()._parse_cancel_result(_cancel_result([never]), req)
        assert out["cancelledCount"] == 0 and out["failedCount"] == 1
        assert "already canceled" in out["error"]
        assert out["outcomes"][0]["status"] == "error"

    def test_mixed_counts_are_accurate(self):
        req = [{"coin": "BTC", "oid": 1}, {"coin": "BTC", "oid": 2}]
        out = _inst()._parse_cancel_result(
            _cancel_result(["success", {"error": "filled"}]), req
        )
        assert out["cancelledCount"] == 1 and out["failedCount"] == 1

    def test_top_level_err_fails_everything(self):
        req = [{"coin": "BTC", "oid": 1}]
        out = _inst()._parse_cancel_result(
            {"status": "err", "response": "User or API Wallet does not exist."}, req
        )
        assert out["cancelledCount"] == 0 and out["failedCount"] == 1
        assert "does not exist" in out["error"]

    def test_missing_status_is_not_success(self):
        # Fewer statuses than requests must not be counted as cancelled.
        req = [{"coin": "BTC", "oid": 1}, {"coin": "BTC", "oid": 2}]
        out = _inst()._parse_cancel_result(_cancel_result(["success"]), req)
        assert out["cancelledCount"] == 1 and out["failedCount"] == 1
        assert out["outcomes"][1]["status"] == "unknown"


# ---------------------------------------------------------------------------
# _parse_bracket_result
# ---------------------------------------------------------------------------


def _bulk_result(statuses):
    return {
        "status": "ok",
        "response": {"type": "order", "data": {"statuses": statuses}},
    }


class TestParseBracketResult:
    def test_all_legs_ok(self):
        infos, failed = _inst()._parse_bracket_result(
            _bulk_result(
                [
                    {"filled": {"oid": 1, "totalSz": "1", "avgPx": "100"}},
                    {"resting": {"oid": 2}},
                    {"resting": {"oid": 3}},
                ]
            )
        )
        assert failed == []
        assert [i["orderType"] for i in infos] == ["entry", "take-profit", "stop-loss"]

    def test_rejected_sl_leg_is_flagged(self):
        infos, failed = _inst()._parse_bracket_result(
            _bulk_result(
                [
                    {"filled": {"oid": 1, "totalSz": "1", "avgPx": "100"}},
                    {"resting": {"oid": 2}},
                    {"error": "Invalid trigger price"},
                ]
            )
        )
        assert len(failed) == 1
        assert failed[0]["orderType"] == "stop-loss"
        # The entry leg's state stays visible so the caller knows cleanup is needed.
        assert infos[0]["status"] == "filled"

    def test_extra_statuses_do_not_crash(self):
        infos, _ = _inst()._parse_bracket_result(
            _bulk_result([{"resting": {"oid": i}} for i in range(4)])
        )
        assert infos[3]["orderType"] == "leg-3"


# ---------------------------------------------------------------------------
# _validate_bracket_geometry
# ---------------------------------------------------------------------------


class TestBracketGeometry:
    def test_valid_long(self):
        S._validate_bracket_geometry(True, 100.0, 110.0, 90.0)  # no raise

    def test_valid_short(self):
        S._validate_bracket_geometry(False, 100.0, 90.0, 110.0)  # no raise

    def test_long_wrong_side_stop_rejected(self):
        # SL above entry on a long would trigger instantly on placement.
        with pytest.raises(ValueError, match="long"):
            S._validate_bracket_geometry(True, 100.0, 110.0, 105.0)

    def test_long_wrong_side_tp_rejected(self):
        with pytest.raises(ValueError, match="long"):
            S._validate_bracket_geometry(True, 100.0, 95.0, 90.0)

    def test_short_wrong_side_stop_rejected(self):
        with pytest.raises(ValueError, match="short"):
            S._validate_bracket_geometry(False, 100.0, 90.0, 95.0)

    def test_equal_prices_rejected(self):
        with pytest.raises(ValueError):
            S._validate_bracket_geometry(True, 100.0, 100.0, 90.0)


# ---------------------------------------------------------------------------
# _get_trades dead-socket guard
# ---------------------------------------------------------------------------

import threading
import time
from collections import deque


def _trades_server(trades, last_recv, rest_returns):
    """Bare instance wired with just the state _get_trades touches."""
    inst = object.__new__(S)
    inst._state_lock = threading.Lock()
    inst._subscribed_trades = {"BTC"}  # skip live subscription
    inst.local_trades = {"BTC": deque(trades, maxlen=1000)}
    inst._trades_last_recv = {"BTC": last_recv}
    inst._recent_trades_rest = lambda coin, cutoff_ms: rest_returns
    return inst


def _t(offset_secs):
    return {"time": (time.time() + offset_secs) * 1000, "px": "1", "sz": "1", "side": "B"}


class TestGetTradesLiveness:
    def test_live_socket_serves_mirror(self):
        inst = _trades_server([_t(-5)], last_recv=time.time() - 1, rest_returns=[])
        trades, source = inst._get_trades("BTC", window_secs=60)
        assert source == "websocket" and len(trades) == 1

    def test_live_socket_empty_window_is_no_flow(self):
        inst = _trades_server([_t(-300)], last_recv=time.time() - 1, rest_returns=[])
        trades, source = inst._get_trades("BTC", window_secs=60)
        assert source == "websocket" and trades == []

    def test_dead_socket_with_residual_window_falls_back_to_rest(self):
        # Socket died mid-window: the deque still holds in-window trades, but
        # the tape is missing its most recent minutes — must NOT be served as
        # fresh websocket data.
        rest_tape = [_t(-1), _t(-2)]
        inst = _trades_server(
            [_t(-50)], last_recv=time.time() - 120, rest_returns=rest_tape
        )
        trades, source = inst._get_trades("BTC", window_secs=3600)
        assert source == "rest" and trades == rest_tape

    def test_cold_start_uses_rest(self):
        inst = object.__new__(S)
        inst._state_lock = threading.Lock()
        inst._subscribed_trades = {"BTC"}
        inst.local_trades = {}
        inst._trades_last_recv = {}
        inst._recent_trades_rest = lambda coin, cutoff_ms: ["primed"]
        trades, source = inst._get_trades("BTC", window_secs=60)
        assert source == "rest" and trades == ["primed"]
