import os
from datetime import datetime
import analyzer


JSON_PATH = "logs/sample_app.json"
LOG_PATH = "logs/sample_app.log"
OUT_DIR = "test_output"


def test_parse_text_log_valid():
    line = "2025-01-01 10:00:00 ERROR payment host2 Payment failed"
    result = analyzer.parse_text_log(line)

    assert result["level"] == "ERROR"
    assert result["service"] == "payment"
    assert result["host"] == "host2"
    assert result["message"] == "Payment failed"
    assert isinstance(result["timestamp"], datetime)


def test_parse_text_log_invalid():
    assert analyzer.parse_text_log("INVALID LINE") is None


def test_parse_text_log_empty_line():
    assert analyzer.parse_text_log("") is None


def test_parse_text_log_invalid_timestamp():
    line = "01-01-2025 10:00 ERROR payment host2 Failed"
    assert analyzer.parse_text_log(line) is None


def test_read_logs_returns_list():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    assert isinstance(logs, list)


def test_read_logs_not_empty():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    assert len(logs) > 0


def test_read_logs_timestamp_type():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    for log in logs:
        assert isinstance(log["timestamp"], datetime)


def test_read_logs_has_required_keys():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    for log in logs:
        assert "level" in log
        assert "service" in log
        assert "host" in log
        assert "message" in log


def test_filter_logs_service_and_host():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    filtered = analyzer.filter_logs(logs, service="payment", host="host2")

    for log in filtered:
        assert log["service"] == "payment"
        assert log["host"] == "host2"


def test_filter_logs_no_match():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    filtered = analyzer.filter_logs(logs, service="unknown", host="unknown")
    assert filtered == []



def test_detect_burst_errors_returns_list():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    bursts = analyzer.detect_burst_errors(logs)
    assert isinstance(bursts, list)


def test_detect_long_running_issues_returns_dict():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    issues = analyzer.detect_long_running_issues(logs)
    assert isinstance(issues, dict)


def test_write_daily_summary_creates_file():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    analyzer.write_daily_summary(logs, OUT_DIR)

    assert os.path.exists(f"{OUT_DIR}/daily_summary.csv")


def test_write_level_csv_creates_files():
    logs = analyzer.read_logs(JSON_PATH, LOG_PATH)
    analyzer.write_level_csv(logs, OUT_DIR)

    assert os.path.exists(f"{OUT_DIR}/ERROR.csv")
    assert os.path.exists(f"{OUT_DIR}/INFO.csv")
