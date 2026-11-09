# Assignment #4: Report

**Name:** Tarang Sonkusare
**Student ID:** 801352372
**Email:** tsonkusa@charlotte.edu

---

## Seed and commands

**Seed:** 801352372

I generated 60,000 historical trips and 30 stream files containing 1,228 rides.

```bash
python3 datagen.py 801352372
docker compose -f docker-compose.codespaces.yml up -d
docker cp main.py spark-master:/opt/spark/work-dir/

# Ran each part separately using:
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py <part> \
  /opt/spark/work-dir/shared 801352372

# Parts executed: part1a, part1b, part2, part3

# Generate ground truth
python3 datagen.py 801352372 shared-folder /tmp/truth.csv
```

---

## Part 1a: Trips and revenue per window

I used five-minute tumbling windows with a three-minute watermark and wrote the results to Parquet in append mode.

**Micro-batch summary (selected batches):**

```text
Batch   Input   Watermark             State Rows
0       36      -                     21
1       42      07:57:53              26
4       44      08:00:51              24
9       45      08:05:55              25
14      45      08:10:59              24
19      42      08:15:58              24
24      39      08:20:46              25
29      43      08:25:53              25
30      0       08:26:49              25
```

The stream wrote windows from 07:55–08:00 through 08:20–08:25. The final 08:25–08:30 window was missing because the watermark never advanced far enough to close it.

Some streaming counts were lower than the batch results:

| Window | Zone | Stream | Batch |
|---|---:|---:|---:|
| 08:00–08:05 | 14 | 7 | 8 |
| 08:00–08:05 | 25 | 7 | 8 |
| 08:05–08:10 | 1 | 22 | 23 |
| 08:10–08:15 | 3 | 23 | 24 |

For zone 14 in the 08:00–08:05 window, streaming revenue was $152.11 versus $188.70 in batch. The generator creates rides arriving up to 8–10 minutes late. Spark drops events that arrive after the watermark has passed their windows, while batch processing includes everything.

State rows drop roughly every five batches because the watermark closes completed five-minute windows and Spark removes their stored aggregation state.

---

## Part 1b: Alerts

The stream generated **10 alerts: 2 high-fare and 8 zero-distance**.

First five alert records, sorted by ride ID:

```text
ride_id  fare    distance_km  reason
R00040   17.73   0.00         zero_distance
R00084   21.54   0.00         zero_distance
R00183   22.14   0.00         zero_distance
R00259  163.97  44.61         high_fare
R00489   31.87   0.00         zero_distance
```

No watermark or state is needed because each ride is checked independently. To detect more than three alerts from the same driver within ten minutes, I would use a stateful window aggregation grouped by driver, along with a watermark for late events.

---

## Part 2: Fare prediction model

The data was split into **47,998 training rows** and **12,002 test rows**.

```text
Model       RMSE    R²
baseline    3.74    0.845
full        1.70    0.968
tuned       1.70    0.968

Best regParam: 0.0
Cross-validation fits: 6
Saved model: full
```

The baseline used distance only. The full model added pickup zone, payment type, pickup hour, and duration, reducing RMSE by about 54.5%.

For tuning, I used LinearRegression with `regParam` values `[0.0, 0.1]` and three-fold cross-validation (`parallelism=2`). I chose these values to compare no regularization against some regularization. Cross-validation performed six parameter-fold fits, plus the final refit.

Tuning did not improve the reported RMSE, suggesting the full linear model already captures the synthetic fare relationships well.

The successful training run began at 23:44:28; I did not record its exact total runtime.

Hour is treated as a category because fares can vary by time of day without increasing linearly with the hour number.

---

## Part 3: Streaming fare anomalies

I loaded the saved model and flagged rides when the difference between their actual and predicted fares exceeded **4 × RMSE = $6.80**.

The model flagged **72 out of 1,228 rides**.

**Top 10 deviations:**

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

**Flagged rides per five-minute window:**

```text
07:55   1
08:00   7
08:05  17
08:10   8
08:15  16
08:20  10
08:25  13
Total  72
```

**Comparison with planted anomalies:**

| Metric | Result |
|---|---:|
| Planted anomalies | 48 |
| True positives | 48 |
| False positives | 24 |
| False negatives | 0 |
| Precision | 66.67% |
| Recall | 100% |

The model detected all 48 planted anomalies, but also flagged 24 normal rides. Six false positives had zero distance, while many others were longer rides where the model underpredicted the fare. For example, R00259 had an actual fare of $163.97 compared with a prediction of $140.08.

To reduce false positives, I would try nonlinear models such as RandomForestRegressor, add interaction features, and handle zero-distance rides separately.

There is no `fit()` in Part 3 because the model was already trained and saved in Part 2. MLlib's batch estimators cannot be fitted directly on this streaming DataFrame.

The loaded pipeline contains three fitted StringIndexerModels (zone, payment type, hour), an OneHotEncoderModel that learned category sizes, a VectorAssembler that combines features, and a LinearRegressionModel that learned the fare coefficients and intercept.

---

## Design: Production pipeline

In production, Kafka would receive live rides, and Spark Structured Streaming would score them using a saved model. A separate nightly Spark job would retrain the model using the previous month's trips stored in S3. After testing, the new model would be versioned and deployed through a controlled restart of the scoring job while keeping the same Kafka offsets and checkpoint to avoid losing events. Alerts would be sent to a fraud team through a database or message queue. I would monitor processing delays, prediction errors, false-positive rates, data drift, and model performance to determine when retraining is needed.

---

## Problems and fixes

**1. Missing NumPy:** Part 1a initially failed with:

```text
ModuleNotFoundError: No module named 'numpy'
```

I fixed this by installing NumPy 1.26.4 in the Spark master and both worker containers.

**2. Cross-validation error:** Part 2 initially failed with:

```text
NameError: name 'regression' is not defined
```

I fixed the parameter grid by referencing the last estimator in `full_pipeline`:

```python
.addGrid(full_pipeline.getStages()[-1].regParam, [0.0, 0.1])
```

**3. Numerical warnings:** Spark reported:

```text
Cholesky solver failed due to singular covariance matrix.
Retrying with Quasi-Newton solver.
```

It also reported `ERROR LBFGS: Failure!` during optimization. These warnings did not prevent the models from producing metrics or the final training job from completing. Regularization or feature adjustments could improve numerical stability.

**4. Spark warnings:** Spark warned about missing native Hadoop libraries and disabled adaptive query execution for streaming. Neither prevented successful execution.

All four assignment parts completed successfully, and the scoring model achieved **100% recall and 66.67% precision** on the planted anomalies.
