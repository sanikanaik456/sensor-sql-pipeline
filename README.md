# Sensor data pipeline with SQL (SQLite)

![tests](https://github.com/sanikanaik456/sensor-sql-pipeline/actions/workflows/tests.yml/badge.svg)



A small, rerunnable pipeline that takes machine temperature readings, checks them, cleans them, loads them into a SQLite database, and answers questions about them with SQL.



## Data

Numenta Anomaly Benchmark (NAB), file `machine_temperature_system_failure`: 22,695 readings, one every 5 minutes, with four labelled failure windows. The data is not included here. To download it:



    Invoke-WebRequest -Uri "https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/machine_temperature_system_failure.csv" -OutFile machine_temperature.csv

    Invoke-WebRequest -Uri "https://raw.githubusercontent.com/numenta/NAB/master/labels/combined_windows.json" -OutFile labels.json



See https://github.com/numenta/NAB for the data and its licence.



## What the pipeline does

1. **Extract:** reads the readings (CSV) and the failure windows (JSON).

2. **Validate:** counts rows, missing values, duplicate timestamps, gaps longer than 5 minutes, and values outside 0 to 150. Problems are flagged and logged, never silently fixed.

3. **Transform:** merges duplicate timestamps by averaging, sorts by time, adds a day column.

4. **Load:** writes three tables to SQLite: `clean_readings`, `failure_windows`, and `quality_log` (one row per check per run, so there is a history).

5. **Report:** runs SQL queries (JOIN, GROUP BY, HAVING and a LAG window function).



The load step is safe to rerun: the clean table is replaced, so running twice does not duplicate readings, while the quality log keeps a record of each run.



## Results on this dataset

- 22,695 rows, no missing values, 12 duplicate timestamps (merged, leaving 22,683 readings), no gaps, nothing outside 0 to 150.

- Only the first day (33 readings) and the last day (186 readings) are incomplete, which agrees with there being no gaps.

- Average temperature inside the failure windows is 64.0, against 88.4 outside.

- Average of each failure window: 79.1, 72.9, 64.7 and 39.5.

- The biggest 1-hour drop is -22.7, and four of the five biggest drops fall inside the second failure window.



## Tests

    pytest -v

Six tests check that each data problem is flagged, that duplicates are merged by averaging, that the data is sorted, that the load can be rerun without duplicating readings, and that the SQL join counts readings inside a window. The tests use a temporary database.



## Limits

- One dataset, and a small one. SQLite is a good way to learn and test SQL, but it is not a production data warehouse.

- There is no scheduler. The pipeline runs when someone runs it.

- The 0 to 150 range is my assumption about what is sensible for this sensor.

- Averaging duplicate readings is a choice. Other choices (keeping the first or last reading) would give slightly different values.

- The "1 hour" in the drops query assumes 12 readings are one hour, which holds here because there are no gaps.



## Run it

    pip install -r requirements.txt

    python pipeline.py

    pytest -v



Data: Numenta Anomaly Benchmark.

