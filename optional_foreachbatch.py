"""Optional extension 1: Query streaming window results with Spark SQL."""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as sround, sum as ssum, window

shared = sys.argv[1].rstrip("/")

spark = (
    SparkSession.builder
    .appName("A4-Optional-ForeachBatch")
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.sql.warehouse.dir", f"{shared}/optional/warehouse")
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
    .json(f"{shared}/stream")
)

windows = (
    rides.withWatermark("event_time", "3 minutes")
    .groupBy(window("event_time", "5 minutes"), "pickup_zone")
    .agg(
        count("*").alias("trips"),
        sround(ssum("fare"), 2).alias("revenue")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "pickup_zone",
        "trips",
        "revenue"
    )
)


def save_and_query(batch_df, batch_id):
    batch_df.persist()
    try:
        rows = batch_df.count()

        if rows == 0:
            print(f"Batch {batch_id}: no finalized windows")
            return

        batch_df.write.mode("append").saveAsTable(
            "zone_window_totals"
        )
        spark.catalog.refreshTable("zone_window_totals")

        print(f"Batch {batch_id}: wrote {rows} rows")
        spark.sql("""
            SELECT window_start, pickup_zone, trips, revenue
            FROM zone_window_totals
            ORDER BY window_start, pickup_zone
            LIMIT 10
        """).show(truncate=False)
    finally:
        batch_df.unpersist()


query = (
    windows.writeStream
    .foreachBatch(save_and_query)
    .outputMode("append")
    .option("checkpointLocation", f"{shared}/optional/checkpoint_foreachbatch")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("\nFinal SQL table row count:")
spark.sql("SELECT COUNT(*) AS rows FROM zone_window_totals").show()

spark.stop()
