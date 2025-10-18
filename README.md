# Spark Streaming Analytics - Hands-on L9

**Student:** Gaurav Patel  
**Course:** ITCS 6190/8190 - Cloud Computing for Data Analysis  
**Semester:** Fall 2025

---

## Overview

Real-time analytics pipeline for a ride-sharing platform using **Apache Spark Structured Streaming**. Processes streaming ride data, performs aggregations, and analyzes trends using time-based windows.

---

## Prerequisites
```bash
# Python 3.x
python3 --version

# Install dependencies
pip install pyspark faker
```

---

## Project Structure
```
Handson-L8-Spark-SQL_Streaming/
├── task1.py                    # Streaming ingestion
├── task2.py                    # Driver aggregations
├── task3.py                    # Windowed analytics
├── data_generator.py           # Data stream simulator
├── outputs/
│   ├── task2/                  # 3 sample CSVs
│   └── task3/                  # 3 sample CSVs
└── README.md
```

---

## How to Run

**Terminal 1 - Start data generator:**
```bash
python data_generator.py
```

**Terminal 2 - Run tasks:**
```bash
# Task 1: View parsed stream (stop after 10-20 records)
python task1.py

# Task 2: Driver aggregations (run 40-50 seconds)
python task2.py

# Task 3: Windowed analytics (run 8-10 minutes minimum)
python task3.py
```

---

## Task 1: Streaming Ingestion

**Objective:** Ingest and parse JSON ride data from socket stream

**Implementation:**
- Connect to `localhost:9999` socket
- Parse schema: `trip_id`, `driver_id`, `distance_km`, `fare_amount`, `timestamp`
- Display real-time data to console

**Sample Output:**
```
+--------------------+---------+-----------+-----------+-------------------+
|             trip_id|driver_id|distance_km|fare_amount|          timestamp|
+--------------------+---------+-----------+-----------+-------------------+
|9c57889d-a31a-43d...|       90|      24.36|      26.77|2025-10-15 23:35:36|
|0ab508ec-ec54-480...|       22|       3.26|       6.80|2025-10-15 23:35:37|
+--------------------+---------+-----------+-----------+-------------------+
```

---

## Task 2: Driver Aggregations

**Objective:** Calculate total fare and average distance per driver

**Implementation:**
- Group by `driver_id`
- Aggregate: `SUM(fare_amount)`, `AVG(distance_km)`
- Output mode: `complete`
- Write to CSV with checkpointing

**Sample Output:**
```csv
driver_id,total_fare,avg_distance
69,87.04,36.68
59,101.44,46.41
96,49.54,38.93
38,114.35,7.21
```

---

## Task 3: Windowed Analytics

**Objective:** Aggregate fares using 5-minute sliding windows

**Implementation:**
- Convert timestamp to `Timestamp` type
- 5-minute window, 1-minute slide, 1-minute watermark
- Aggregate: `SUM(fare_amount)` per window
- Output mode: `append`

**Sample Output:**
```csv
window_start,window_end,sum_fare
2025-10-15T23:45:00.000Z,2025-10-15T23:50:00.000Z,7111.25
2025-10-15T23:46:00.000Z,2025-10-15T23:51:00.000Z,12445.29
2025-10-15T23:48:00.000Z,2025-10-15T23:53:00.000Z,21038.08
```

**Note:** Run for 8-10 minutes minimum. Windows need time to complete before producing data.

---

## Technologies

- Apache Spark 3.x (Structured Streaming)
- PySpark (Python API)
- Faker (Data generation)

---

## Results Summary

- ✅ Ingested 400+ streaming records
- ✅ Real-time driver performance tracking
- ✅ Time-windowed revenue analytics
- ✅ Clean CSV outputs (3 samples per task)

---

**Repository:** https://github.com/Gaurav23p24/Handson-L8-Spark-SQL_Streaming
