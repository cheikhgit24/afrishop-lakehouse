import json
import logging

from afrishop.logging_utils import JsonFormatter, get_logger, log_event


def test_json_formatter_outputs_valid_json_with_extra_fields():
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", None, None)
    record.extra_fields = {"rows": 3}
    payload = json.loads(JsonFormatter().format(record))
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["rows"] == 3
    assert "ts" in payload


def test_json_formatter_includes_exception():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "fail", None, sys.exc_info())
    assert "boom" in json.loads(JsonFormatter().format(record))["exception"]


def test_get_logger_is_idempotent_and_log_event_writes_json(capsys):
    a = get_logger("afrishop.test")
    b = get_logger("afrishop.test")
    assert a is b and len(a.handlers) == 1
    log_event(a, "event", table="orders", rows=10)
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["message"] == "event" and out["table"] == "orders" and out["rows"] == 10
