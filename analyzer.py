import re
import json
import csv
import os
import argparse
from datetime import datetime
from collections import defaultdict



def parse_text_log(line):
    pattern = r"(\S+ \S+) (\S+) (\S+) (\S+) (.+)"
    match = re.match(pattern, line)

    if not match:
        return None

    try:
        timestamp = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    return {
        "timestamp": timestamp,
        "level": match.group(2),
        "service": match.group(3),
        "host": match.group(4),
        "message": match.group(5)
    }


def read_logs(json_path, log_path):
    logs = []

    # Validate input paths
    if not json_path and not log_path:
        raise ValueError("At least one input file must be provided")

    # Read JSON logs
    if json_path:
        if not os.path.exists(json_path):
            print(f"WARNING: {json_path} not found")
        else:
            with open(json_path, encoding="utf-8") as f:
                json_logs = json.load(f)
                for log in json_logs:
                    log["timestamp"] = datetime.fromisoformat(log["timestamp"])
                    logs.append(log)

    # Read text logs
    if log_path:
        if not os.path.exists(log_path):
            print(f"WARNING: {log_path} not found")
        else:
            with open(log_path, encoding="utf-8") as f:
                for line in f:
                    parsed = parse_text_log(line.strip())
                    if parsed:
                        logs.append(parsed)

    return logs


def filter_logs(logs, service=None, host=None):
    return [
        log for log in logs
        if (not service or log["service"] == service)
        and (not host or log["host"] == host)
    ]


def detect_burst_errors(logs):
    error_times = sorted(
        [log["timestamp"] for log in logs if log["level"] == "ERROR"]
    )

    bursts = []
    for i in range(len(error_times) - 4):
        if (error_times[i + 4] - error_times[i]).total_seconds() <= 60:
            bursts.append(error_times[i:i + 5])

    return bursts


def detect_long_running_issues(logs):
    error_days = defaultdict(set)

    for log in logs:
        if log["level"] == "ERROR":
            error_days[log["message"]].add(log["timestamp"].date())

    return {
        msg: days
        for msg, days in error_days.items()
        if len(days) > 1
    }


def write_daily_summary(logs, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    summary = defaultdict(lambda: defaultdict(int))

    for log in logs:
        summary[log["timestamp"].date()][log["level"]] += 1

    with open(os.path.join(out_dir, "daily_summary.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "level", "count"])

        for day, levels in summary.items():
            for level, count in levels.items():
                writer.writerow([day, level, count])


def write_level_csv(logs, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    if not logs:
        return

    files = {}

    for log in logs:
        level = log["level"]

        if level not in files:
            f = open(os.path.join(out_dir, f"{level}.csv"), "w", newline="", encoding="utf-8")
            writer = csv.writer(f)
            writer.writerow(["timestamp", "service", "host", "message"])
            files[level] = (f, writer)

        files[level][1].writerow([
            log["timestamp"],
            log["service"],
            log["host"],
            log["message"]
        ])

    for f, _ in files.values():
        f.close()


def main():
    parser = argparse.ArgumentParser(description="Log File Analyzer")

    parser.add_argument("--json", help="Path to JSON log file")
    parser.add_argument("--log", help="Path to text log file")
    parser.add_argument("--out", default="output", help="Output folder")
    parser.add_argument("--service", help="Filter by service")
    parser.add_argument("--host", help="Filter by host")

    args = parser.parse_args()

    print("PROGRAM STARTED")

    logs = read_logs(args.json, args.log)
    print("Logs loaded:", len(logs))

    logs = filter_logs(logs, service=args.service, host=args.host)
    print("Logs after filter:", len(logs))

    write_daily_summary(logs, args.out)
    write_level_csv(logs, args.out)

    bursts = detect_burst_errors(logs)
    issues = detect_long_running_issues(logs)

    print("Burst errors detected:", len(bursts))
    print("Long running issues detected:", len(issues))
    print("CSV files written to:", args.out)
    print("PROGRAM FINISHED")


if __name__ == "__main__":
    main()

