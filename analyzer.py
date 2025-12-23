import re
import json
import csv
from datetime import datetime
from collections import defaultdict
import os

# Ensure output folder exists
os.makedirs("output", exist_ok=True)

# --------------------------------
# Parse plain text log
# --------------------------------
def parse_text_log(line):
    pattern = r"(\S+ \S+) (\S+) (\S+) (\S+) (.+)"
    match = re.match(pattern, line)

    if not match:
        return None

    return {
    "timestamp": datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S"),
    "level": match.group(2),
    "service": match.group(3),
    "host": match.group(4),
    "message": match.group(5)
}


# --------------------------------
# Read all logs (JSON + TEXT)
# --------------------------------
def read_logs():
    logs = []

    # Read JSON logs
    with open("logs/app.json") as f:
        json_logs = json.load(f)
        for log in json_logs:
            log["timestamp"] = datetime.fromisoformat(log["timestamp"])
            logs.append(log)

    # Read text logs
    with open("logs/app.log") as f:
        for line in f:
            parsed = parse_text_log(line.strip())
            if parsed:
                logs.append(parsed)

    return logs

# --------------------------------
# Filter logs
# --------------------------------
def filter_logs(logs, service=None, host=None):
    return [
        log for log in logs
        if (not service or log["service"] == service)
        and (not host or log["host"] == host)
    ]

# --------------------------------
# Detect burst errors
# --------------------------------
def detect_burst_errors(logs):
    error_times = sorted(
        [log["timestamp"] for log in logs if log["level"] == "ERROR"]
    )

    bursts = []
    for i in range(len(error_times) - 4):
        if (error_times[i + 4] - error_times[i]).seconds <= 60:
            bursts.append(error_times[i:i + 5])

    return bursts

# --------------------------------
# Detect long running issues
# --------------------------------
def detect_long_running_issues(logs):
    error_days = defaultdict(set)

    for log in logs:
        if log["level"] == "ERROR":
            error_days[log["message"]].add(log["timestamp"].date())

    return {msg: days for msg, days in error_days.items() if len(days) > 1}

# --------------------------------
# Daily summary CSV
# --------------------------------
def write_daily_summary(logs):
    summary = defaultdict(lambda: defaultdict(int))

    for log in logs:
        summary[log["timestamp"].date()][log["level"]] += 1

    with open("output/daily_summary.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "level", "count"])

        for day, levels in summary.items():
            for level, count in levels.items():
                writer.writerow([day, level, count])

# --------------------------------
# Separate CSV per log level
# --------------------------------
def write_level_csv(logs):
    files = {}

    for log in logs:
        level = log["level"]

        if level not in files:
            f = open(f"output/{level}.csv", "w", newline="")
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

# --------------------------------
# Main
# --------------------------------
def main():
    print("PROGRAM STARTED")

    logs = read_logs()
    print("Logs loaded:", len(logs))

    for log in logs:
        print(log)

    logs = filter_logs(logs, service="payment", host="host2")
    print("Logs after filter:", len(logs))

    write_daily_summary(logs)
    print("daily_summary.csv written")

    write_level_csv(logs)
    print("level CSVs written")

    print("PROGRAM FINISHED")

if __name__ == "__main__":
    main()
