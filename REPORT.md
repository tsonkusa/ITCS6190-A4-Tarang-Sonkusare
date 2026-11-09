# Assignment #4: Report

**Name:** Tarang Sonkusare
**Student ID:** 801352372
**Email:** tsonkusa@charlotte.edu

---

## Seed and commands

**Seed:** 801352372

I generated 60,000 historical trips and 30 stream files with 1,228 rides.

```bash
python3 datagen.py 801352372
docker compose -f docker-compose.codespaces.yml up -d
docker cp main.py spark-master:/opt/spark/work-dir/

# Run separately for part1a, part1b, part2, and part3:
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py <part> \
  /opt/spark/work-dir/shared 801352372

# Generate planted-anomaly ground truth for the report:
python3 datagen.py 801352372 shared-folder /tmp/truth.csv
```

I installed NumPy in the three Spark containers after a dependency error and corrected the tuning grid before the successful training run.

---

## Part 1a: Trips and revenue per window

I grouped rides by pickup zone and five-minute event-time windows, used a three-minute watermark, and wrote the finalized windows in append mode to Parquet.

**Selected micro-batches:**

```text
Batch   Input   Watermark    State rows
0       36      -            21
1       42      07:57:53     26
4       44      08:00:51     24
9       45      08:05:55     25
14      45      08:10:59     24
19      42      08:15:58     24
24      39      08:20:46     25
29      43      08:25:53     25
30       0      08:26:49     25
```

The output included windows from 07:55–08:00 through 08:20–08:25. The 08:25–08:30 window was absent because the final watermark (08:26:49) had not passed its end.

The program prints only the first 100 rows of each table (`show(100)`). Below are **all 15 differences in those displayed rows**; this does not claim to be an exhaustive comparison of the full outputs.

| Window | Zone | Stream trips | Batch trips | Stream revenue | Batch revenue |
|---|---:|---:|---:|---:|---:|
| 08:00–08:05 | 14 | 7 | 8 | 152.11 | 188.70 |
| 08:00–08:05 | 25 | 7 | 8 | 157.14 | 193.46 |
| 08:05–08:10 | 1 | 22 | 23 | 474.38 | 492.58 |
| 08:05–08:10 | 4 | 7 | 8 | 158.22 | 165.67 |
| 08:05–08:10 | 11 | 8 | 9 | 140.28 | 175.68 |
| 08:05–08:10 | 23 | 9 | 10 | 195.77 | 229.42 |
| 08:10–08:15 | 3 | 23 | 24 | 534.80 | 558.29 |
| 08:10–08:15 | 11 | 17 | 18 | 331.09 | 343.13 |
| 08:10–08:15 | 14 | 8 | 9 | 223.44 | 232.33 |
| 08:10–08:15 | 23 | 7 | 8 | 105.26 | 118.82 |
| 08:15–08:20 | 3 | 23 | 24 | 574.09 | 594.21 |
| 08:15–08:20 | 5 | 16 | 18 | 438.30 | 518.34 |
| 08:15–08:20 | 11 | 7 | 8 | 208.28 | 220.67 |
| 08:15–08:20 | 13 | 8 | 10 | 188.64 | 225.06 |
| 08:15–08:20 | 14 | 12 | 14 | 230.07 | 259.48 |

For example, the stream has seven rides and $152.11 for zone 14 at 08:00–08:05, compared with eight rides and $188.70 in batch. The generator deliberately sends some records 8–10 minutes late. Once the watermark passes their event-time window, Spark excludes them from the streaming count, while batch aggregation still includes them. State rows rise as windows accumulate, then fall about every five batches as completed windows are output and their state is removed. The last unfinished window still has state when the query ends.

---

## Part 1b: Alerts

The stream produced **10 alerts: two high fares and eight zero-distance rides**. First five alerts, sorted by ride ID:

```csv
ride_id,event_time,pickup_zone,distance_km,duration_min,fare,reason
R00040,2026-10-27 08:01:22,7,0.0,13.0,17.73,zero_distance
R00084,2026-10-27 08:02:49,9,0.0,13.9,21.54,zero_distance
R00183,2026-10-27 08:04:07,23,0.0,18.5,22.14,zero_distance
R00259,2026-10-27 08:05:50,12,44.61,164.1,163.97,high_fare
R00489,2026-10-27 08:09:52,13,0.0,32.5,31.87,zero_distance
```

No watermark or state was needed because each ride was checked independently. To flag drivers with more than three alerts within ten minutes, I would group alerts by driver and time window and use a watermark to manage late events and state cleanup.

---

## Part 2: Fare prediction model

I trained on **47,998 rows** and tested on **12,002 rows**.

```text
Model       RMSE    R²
baseline    3.74    0.845
full        1.70    0.968
tuned       1.70    0.968

Best regParam: 0.0
Cross-validation: 2 parameters × 3 folds = 6 fits, plus final refit
Saved model: full
```

The baseline used only distance. The full pipeline added zone, payment type, pickup hour, and duration, cutting RMSE by about 54.5%. I tuned the full LinearRegression pipeline with `regParam` values `0.0` and `0.1`, using three-fold cross-validation and parallelism of two. The best setting was 0.0, but tuning did not beat the full model at the displayed precision. The synthetic fares seem to fit a linear model well already.

The successful training run started at 23:44:28, but I did not record its end time, so the exact duration is unavailable. Hour is converted to a string for categorical encoding: fares can vary by hour without increasing linearly as the hour number increases.

---

## Part 3: Streaming fare anomalies

I loaded the saved pipeline and flagged rides with an absolute actual-versus-predicted fare deviation greater than **4 × $1.70 = $6.80**. It flagged **72 of 1,228 rides**.

**Ten largest deviations:**

| Ride | Actual | Predicted | Deviation |
|---|---:|---:|---:|
| R01002 | 178.26 | 63.54 | 114.72 |
| R00755 | 146.66 | 56.38 | 90.28 |
| R00462 | 124.04 | 43.35 | 80.69 |
| R00998 | 104.81 | 41.11 | 63.70 |
| R00352 | 90.02 | 31.94 | 58.08 |
| R00319 | 91.70 | 34.99 | 56.71 |
| R00901 | 73.73 | 25.95 | 47.78 |
| R01143 | 65.13 | 22.63 | 42.50 |
| R00525 | 59.90 | 20.55 | 39.35 |
| R00102 | 65.63 | 26.58 | 39.05 |

**Flags by five-minute window:**

```text
07:55  1
08:00  7
08:05 17
08:10  8
08:15 16
08:20 10
08:25 13
Total 72
```

Ground truth contained **48 planted anomalies**. I caught all 48 (**100% recall**) and flagged 24 rides that were not planted (**66.67% precision**). Six false positives had zero distance, and many others were longer rides with underpredicted fares. For example, R00259 cost $163.97 but was predicted at $140.08. I would test nonlinear features or a tree-based regressor, handle zero-distance trips separately, and choose a threshold based on validation precision and recall.

Part 3 uses `transform()`, not `fit()`, because it scores incoming rides with a model trained on historical batch data. Batch MLlib estimators cannot be directly fitted on this streaming DataFrame. The loaded pipeline has three fitted StringIndexerModels (zone, payment type, hour), a fitted OneHotEncoderModel (category sizes), a configured VectorAssembler (combines features), and a fitted LinearRegressionModel (coefficients and intercept).

---

## Design: Production pipeline

Kafka would receive live rides and Spark Structured Streaming would score them with the saved pipeline. A separate nightly Spark job would retrain on the previous month's completed trips in S3, evaluate the new version, and publish it to versioned model storage. A controlled scoring-job restart would load the approved model while reusing durable Kafka offsets and checkpoint state to avoid skipping events. Alerts would go to the fraud team through a message queue or database. I would monitor throughput, consumer lag, failures, feature drift, RMSE, recall, and false-positive rate to spot stale models.

---

## Problems and fixes

- **Missing dependency:** `ModuleNotFoundError: No module named 'numpy'`. Installed NumPy 1.26.4 in the Spark master and both workers.
- **Tuning variable:** `NameError: name 'regression' is not defined`. Fixed the grid with `.addGrid(full_pipeline.getStages()[-1].regParam, [0.0, 0.1])`.
- **Numerical warnings:** `Cholesky solver failed due to singular covariance matrix. Retrying with Quasi-Newton solver.` and `ERROR LBFGS: Failure!`. The job still finished and produced metrics; further regularization or feature cleanup may help.
- **Spark environment warnings:** Native Hadoop libraries were unavailable, and adaptive query execution was disabled for streaming. Neither prevented completion.

All four parts ran successfully and generated their required output files.
