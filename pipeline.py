"""Small sensor data pipeline: extract, validate, load into SQLite, report with SQL."""
import json
import sqlite3
from datetime import datetime

import pandas as pd

DATA = "machine_temperature.csv"
LABELS = "labels.json"
KEY = "realKnownCause/machine_temperature_system_failure.csv"
DB = "sensor.db"
EXPECTED_STEP = pd.Timedelta(minutes=5)
SANE_MIN, SANE_MAX = 0, 150          # readings outside this range are flagged, not deleted


def extract():
    readings = pd.read_csv(DATA, parse_dates=["timestamp"])
    windows = json.load(open(LABELS))[KEY]
    return readings, windows


def validate(readings):
    """Return a list of (check, value, status). Nothing is silently fixed here."""
    checks = []
    checks.append(("rows", len(readings), "info"))
    missing = int(readings["value"].isna().sum())
    checks.append(("missing_values", missing, "ok" if missing == 0 else "warn"))
    dupes = int(readings["timestamp"].duplicated().sum())
    checks.append(("duplicate_timestamps", dupes, "ok" if dupes == 0 else "warn"))
    ordered = readings.drop_duplicates("timestamp").sort_values("timestamp")
    gaps = int((ordered["timestamp"].diff().dropna() > EXPECTED_STEP).sum())
    checks.append(("gaps_longer_than_5_min", gaps, "ok" if gaps == 0 else "warn"))
    out_of_range = int(((readings["value"] < SANE_MIN) | (readings["value"] > SANE_MAX)).sum())
    checks.append(("values_outside_0_to_150", out_of_range, "ok" if out_of_range == 0 else "warn"))
    return checks


def transform(readings):
    """Merge duplicate timestamps by averaging, sort, add day."""
    clean = (readings.groupby("timestamp", as_index=False)["value"].mean()
             .sort_values("timestamp").reset_index(drop=True))
    clean["day"] = clean["timestamp"].dt.strftime("%Y-%m-%d")
    clean["timestamp"] = clean["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return clean


def load(clean, windows, checks):
    run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB) as con:
        clean.to_sql("clean_readings", con, if_exists="replace", index=False)
        pd.DataFrame(windows, columns=["start_time", "end_time"]).assign(
            start_time=lambda d: pd.to_datetime(d["start_time"]).dt.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=lambda d: pd.to_datetime(d["end_time"]).dt.strftime("%Y-%m-%d %H:%M:%S"),
        ).to_sql("failure_windows", con, if_exists="replace", index=False)
        log = pd.DataFrame(checks, columns=["check_name", "value", "status"]).assign(run_id=run_id)
        log.to_sql("quality_log", con, if_exists="append", index=False)
        con.execute("CREATE INDEX IF NOT EXISTS idx_ts ON clean_readings(timestamp)")
    return run_id


REPORTS = {
    "Readings per day (days with fewer than 288 readings are incomplete)": """
        SELECT day, COUNT(*) AS readings, ROUND(AVG(value), 1) AS avg_temp,
               ROUND(MIN(value), 1) AS min_temp, ROUND(MAX(value), 1) AS max_temp
        FROM clean_readings
        GROUP BY day
        HAVING readings < 288
        ORDER BY day
        LIMIT 5
    """,
    "Temperature inside and outside the labelled failure windows": """
        SELECT CASE WHEN w.start_time IS NULL THEN 'outside windows' ELSE 'inside windows' END AS period,
               COUNT(*) AS readings, ROUND(AVG(r.value), 1) AS avg_temp, ROUND(MIN(r.value), 1) AS min_temp
        FROM clean_readings r
        LEFT JOIN failure_windows w ON r.timestamp BETWEEN w.start_time AND w.end_time
        GROUP BY period
    """,
    "Average temperature of each failure window": """
        SELECT w.start_time, w.end_time, COUNT(*) AS readings, ROUND(AVG(r.value), 1) AS avg_temp,
               ROUND(MIN(r.value), 1) AS min_temp
        FROM failure_windows w
        JOIN clean_readings r ON r.timestamp BETWEEN w.start_time AND w.end_time
        GROUP BY w.start_time, w.end_time
        ORDER BY w.start_time
    """,
    "Five biggest 1-hour temperature drops (12 readings = 1 hour, as there are no gaps)": """
        SELECT timestamp, temp, change_1h
        FROM (
            SELECT timestamp, ROUND(value, 1) AS temp,
                   ROUND(value - LAG(value, 12) OVER (ORDER BY timestamp), 1) AS change_1h
            FROM clean_readings
        )
        WHERE change_1h IS NOT NULL
        ORDER BY change_1h ASC
        LIMIT 5
    """,
}


def report():
    with sqlite3.connect(DB) as con:
        for title, sql in REPORTS.items():
            print(f"\n--- {title} ---")
            print(pd.read_sql_query(sql, con).to_string(index=False))


def main():
    readings, windows = extract()
    checks = validate(readings)
    print("Quality checks:")
    for name, value, status in checks:
        print(f"  {name}: {value} [{status}]")
    clean = transform(readings)
    run_id = load(clean, windows, checks)
    print(f"\nLoaded {len(clean)} clean readings into {DB} (run {run_id})")
    report()


if __name__ == "__main__":
    main()