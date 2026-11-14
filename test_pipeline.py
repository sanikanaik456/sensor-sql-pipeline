import sqlite3

import pandas as pd
import pytest

import pipeline


def make_readings(rows):
    return pd.DataFrame({"timestamp": pd.to_datetime([r[0] for r in rows]), "value": [r[1] for r in rows]})


GOOD = make_readings([
    ("2014-01-01 00:00:00", 80.0),
    ("2014-01-01 00:05:00", 81.0),
    ("2014-01-01 00:10:00", 82.0),
])


def as_dict(checks):
    return {name: (value, status) for name, value, status in checks}


def test_clean_data_passes_every_check():
    result = as_dict(pipeline.validate(GOOD))
    assert result["missing_values"] == (0, "ok")
    assert result["duplicate_timestamps"] == (0, "ok")
    assert result["gaps_longer_than_5_min"] == (0, "ok")
    assert result["values_outside_0_to_150"] == (0, "ok")


def test_validate_flags_each_problem():
    bad = make_readings([
        ("2014-01-01 00:00:00", 80.0),
        ("2014-01-01 00:00:00", 82.0),      # duplicate timestamp
        ("2014-01-01 00:05:00", None),      # missing value
        ("2014-01-01 00:20:00", 500.0),     # 15 minute gap, and outside 0 to 150
    ])
    result = as_dict(pipeline.validate(bad))
    assert result["duplicate_timestamps"] == (1, "warn")
    assert result["missing_values"] == (1, "warn")
    assert result["gaps_longer_than_5_min"] == (1, "warn")
    assert result["values_outside_0_to_150"] == (1, "warn")


def test_transform_merges_duplicates_by_averaging():
    data = make_readings([
        ("2014-01-01 00:00:00", 80.0),
        ("2014-01-01 00:00:00", 82.0),
        ("2014-01-01 00:05:00", 90.0),
    ])
    clean = pipeline.transform(data)
    assert len(clean) == 2
    assert clean.loc[0, "value"] == 81.0
    assert clean.loc[0, "day"] == "2014-01-01"


def test_transform_sorts_by_time():
    data = make_readings([
        ("2014-01-01 00:10:00", 82.0),
        ("2014-01-01 00:00:00", 80.0),
    ])
    clean = pipeline.transform(data)
    assert list(clean["timestamp"]) == ["2014-01-01 00:00:00", "2014-01-01 00:10:00"]


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "DB", str(tmp_path / "test.db"))
    return pipeline.DB


def test_load_is_safe_to_rerun(temp_db):
    clean = pipeline.transform(GOOD)
    windows = [["2014-01-01 00:00:00.000000", "2014-01-01 00:05:00.000000"]]
    checks = pipeline.validate(GOOD)
    pipeline.load(clean, windows, checks)
    pipeline.load(clean, windows, checks)
    with sqlite3.connect(temp_db) as con:
        clean_rows = con.execute("SELECT COUNT(*) FROM clean_readings").fetchone()[0]
        log_rows = con.execute("SELECT COUNT(*) FROM quality_log").fetchone()[0]
    assert clean_rows == 3                      # no duplicate readings after two runs
    assert log_rows == 2 * len(checks)          # the quality log keeps a history of both runs


def test_window_join_counts_readings_inside_window(temp_db):
    clean = pipeline.transform(GOOD)
    windows = [["2014-01-01 00:00:00.000000", "2014-01-01 00:05:00.000000"]]
    pipeline.load(clean, windows, pipeline.validate(GOOD))
    with sqlite3.connect(temp_db) as con:
        inside = con.execute(
            "SELECT COUNT(*) FROM clean_readings r JOIN failure_windows w "
            "ON r.timestamp BETWEEN w.start_time AND w.end_time"
        ).fetchone()[0]
    assert inside == 2