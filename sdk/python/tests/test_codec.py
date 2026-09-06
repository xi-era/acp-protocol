"""Codec unit tests (spec §3, §7.1, §12)."""

import pytest

from acp.codec import (
    COMPONENT_ID_RE,
    PROTOCOL_VERSION,
    error_envelope,
    is_reserved_op,
    is_valid_component_id,
    is_version_supported,
    parse_version,
    validate_envelope,
    validate_reserved_input,
)
from acp.errors import AcpErrorCode


class TestComponentId:
    @pytest.mark.parametrize(
        "value",
        ["sensor.temperature", "a.b", "sensor.temperature.avg.max", "a-1.b-2", "x" * 63 + ".y"],
    )
    def test_valid(self, value):
        assert is_valid_component_id(value)
        assert COMPONENT_ID_RE.match(value)

    @pytest.mark.parametrize(
        "value",
        [
            "sensor",           # single segment
            "Sensor.temp",      # uppercase
            "1sensor.temp",     # segment starts with digit
            "sensor.temp.a.b.c",  # 5 segments
            "sensor..temp",     # empty segment
            "sensor.tem_p",     # underscore
            "",                 # empty
            "sensor .temp",     # space
            42,                 # non-string
            None,
            "sensor.ºtemp",     # non-ascii
        ],
    )
    def test_invalid(self, value):
        assert not is_valid_component_id(value)

    def test_max_segment_length(self):
        assert is_valid_component_id("a" * 63 + "." + "b")
        assert not is_valid_component_id("a" * 64 + ".b")


class TestVersions:
    def test_parse_version(self):
        assert parse_version("0.2") == (0, 2)
        assert parse_version("10.3") == (10, 3)
        assert parse_version("x.y") is None
        assert parse_version("0.2.1") is None
        assert parse_version(42) is None

    def test_support_rule(self):
        # major equal + server minor >= client minor (spec §12.1)
        assert is_version_supported("0.2", "0.2")
        assert is_version_supported("0.1", "0.2")   # 0.1 client on 0.2 server
        assert is_version_supported("0.2", "0.3")
        assert not is_version_supported("0.2", "0.1")  # 0.2 client on 0.1 server
        assert not is_version_supported("1.0", "0.9")
        assert not is_version_supported("garbage", "0.2")


class TestReservedOps:
    def test_is_reserved_op(self):
        assert is_reserved_op("$ping")
        assert is_reserved_op("$subscribe")
        assert is_reserved_op("$unsubscribe")
        assert not is_reserved_op("discover")
        assert not is_reserved_op("$nope")

    def test_validate_reserved_input(self):
        assert validate_reserved_input("$ping", {"ts": 1}) is None
        assert validate_reserved_input("$ping", None) is None
        assert validate_reserved_input("$subscribe", {"component": "a.b"}) is None
        assert validate_reserved_input("$subscribe", {"tags": ["iot"]}) is None
        assert validate_reserved_input("$unsubscribe", None) is None  # unsubscribe all
        # both or neither -> invalid (spec v0.2 §4.4)
        assert validate_reserved_input("$subscribe", {"component": "a.b", "tags": ["t"]}) is not None
        assert validate_reserved_input("$subscribe", {}) is not None
        assert validate_reserved_input("$subscribe", None) is not None
        assert validate_reserved_input("$subscribe", {"component": "BAD"}) is not None
        assert validate_reserved_input("$subscribe", {"tags": []}) is not None
        assert validate_reserved_input("$subscribe", {"tags": [1]}) is not None


class TestValidateEnvelope:
    def test_valid(self):
        result = validate_envelope({"acp": "0.2", "id": "r1", "op": "discover"})
        assert result.ok is True
        assert result.request["id"] == "r1"

    @pytest.mark.parametrize(
        "raw,code",
        [
            ("string", AcpErrorCode.INVALID_ENVELOPE),
            (123, AcpErrorCode.INVALID_ENVELOPE),
            ({"id": "r1", "op": "discover"}, AcpErrorCode.INVALID_ENVELOPE),  # no acp
            ({"acp": "0.2", "op": "discover"}, AcpErrorCode.INVALID_ENVELOPE),  # no id
            ({"acp": "0.2", "id": "r1"}, AcpErrorCode.INVALID_ENVELOPE),  # no op
            ({"acp": "0.2", "id": "r1", "op": "wat"}, AcpErrorCode.UNKNOWN_OP),
            ({"acp": "0.2", "id": "r1", "op": "call"}, AcpErrorCode.INVALID_ENVELOPE),  # call w/o component
            ({"acp": "0.2", "id": "r1", "op": "call", "component": "BAD"}, AcpErrorCode.INVALID_COMPONENT_ID),
            ({"acp": "0.2", "id": "r1", "op": "$subscribe", "input": {}}, AcpErrorCode.INVALID_ENVELOPE),
            ({"acp": "0.2", "id": "r1", "op": "$subscribe", "input": None}, AcpErrorCode.INVALID_ENVELOPE),
            ({"acp": "0.2", "id": "r1", "op": "discover", "stream": "yes"}, AcpErrorCode.INVALID_ENVELOPE),
            ({"acp": "0.2", "id": "r1", "op": "discover", "tags": "iot"}, AcpErrorCode.INVALID_ENVELOPE),
        ],
    )
    def test_failures(self, raw, code):
        result = validate_envelope(raw)
        assert result.ok is False
        assert result.code == code

    def test_error_carries_request_id_when_readable(self):
        result = validate_envelope({"acp": "0.2", "id": "known", "op": "$subscribe", "input": {}})
        assert result.id == "known"
        result = validate_envelope({"acp": "0.2", "op": "discover"})  # id missing
        assert result.id is None


class TestErrorEnvelope:
    def test_shape(self):
        frame = error_envelope("r1", 42200, "input validation failed", {"errors": ["x"]}, "0.1")
        assert frame == {
            "acp": "0.1",
            "id": "r1",
            "ok": False,
            "error": {"code": 42200, "message": "input validation failed", "data": {"errors": ["x"]}},
        }

    def test_data_omitted(self):
        frame = error_envelope(None, 40000, "bad json")
        assert "data" not in frame["error"]
        assert frame["id"] is None
        assert frame["acp"] == PROTOCOL_VERSION
