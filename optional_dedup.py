"""Optional extension 2: Deduplicate rides before ML anomaly scoring."""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, abs as sabs, round as sround, lit
)
from pyspark.ml import PipelineModel

if len(sys.argv) not in (2, 3):
    sys.exit("Usage: optional_dedup.py SHARED_DIR [STREAM_DIR]")

shared = sys.argv[1].rstrip("/")
stream_dir = (
    sys.argv[2].rstrip("/")
    if len(sys.argv) == 3
    else f"{shared}/stream"
)

spark = (
    SparkSession.builder
    .appName("A4-Optional-Dedup")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = (
    "ride_id STRING, driver_id STRING, pickup_zone INT, "
    "distance_km DOUBLE, duration_min DOUBLE, "
    "payment_type STRING, fare DOUBLE, event_time TIMESTAMP"
)

rides = (
    spark.readStream.schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json(stream_dir)
)

# Extension: retain the first observed ride ID.
# The watermark also filters excessively late event-time records.
unique_rides = (
    rides
    .withWatermark("event_time", "3 minutes")
    .dropDuplicates(["ride_id"])
)

featured = (
    unique_rides
    .withColumn("hour", hour(col("event_time")).cast("string"))
    .withColumn("zone", col("pickup_zone").cast("string"))
)

model = PipelineModel.load(f"{shared}/models/fare")
predictions = model.transform(featured)

metrics = spark.read.csv(
    f"{shared}/output/part2/metrics",
    header=True,
    inferSchema=True
)
rmse = float(metrics.orderBy("rmse").first()["rmse"])
threshold = 4 * rmse

anomalies = (
    predictions
    .withColumn(
        "deviation",
        sround(sabs(col("fare") - col("prediction")), 2)
    )
    .filter(col("deviation") > lit(threshold))
    .select(
        "ride_id",
        "event_time",
        "pickup_zone",
        "distance_km",
        "duration_min",
        "payment_type",
        "fare",
        sround(col("prediction"), 2).alias("prediction"),
        "deviation"
    )
)

print(f"Anomaly threshold: {threshold:.2f}")

query = (
    anomalies.writeStream
    .format("csv")
    .outputMode("append")
    .option("header", True)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .option("path", f"{shared}/optional/dedup_output")
    .option(
        "checkpointLocation",
        f"{shared}/optional/checkpoint_dedup"
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("\n=== Deduplication statistics ===")
for progress in query.recentProgress:
    for operator in progress.get("stateOperators", []):
        print(
            f"Batch {progress['batchId']}: "
            f"duplicates dropped = "
            f"{operator.get('numRowsDroppedByWatermark', 0)} late rows, "
            f"state rows = {operator.get('numRowsTotal', 0)}"
        )

print("\nDeduplicated scoring completed.")
spark.stop()
