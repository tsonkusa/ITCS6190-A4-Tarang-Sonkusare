# Assignment 4 — Optional Streaming Extensions

These extensions demonstrate additional Apache Spark Structured Streaming capabilities without modifying the required assignment implementation.

## Extension 1: foreachBatch with Spark SQL

**Script:** `optional_foreachbatch.py`

Implemented a `foreachBatch` sink for the Part 1a five-minute window aggregations.

- Three-minute event-time watermark
- Five-minute tumbling windows grouped by pickup zone
- Append results to the Spark SQL table `zone_window_totals`
- Refresh table metadata after writes
- Cache and uncache each micro-batch to support multiple actions
- Separate checkpoint and warehouse directories

**Verification:** 129 finalized window rows were written and successfully queried using Spark SQL.

## Extension 2: Watermark-Based Deduplication

**Script:** `optional_dedup.py`

Added a three-minute watermark and `dropDuplicates(["ride_id"])` before model inference.

The existing trained PipelineModel was reused without retraining.

**Test:**
- Created two JSON files containing the same ride, `R00837`
- Both files were processed through the streaming pipeline
- Exactly one anomaly record was written
- Anomaly threshold: approximately 6.80

**Result:** Duplicate anomaly scoring was prevented.

**Limitation:** ID-only `dropDuplicates` retains seen-ID state; the event-time watermark does not provide a guaranteed three-minute eviction period for those IDs.

## Extension 3: Continuous Micro-Batch Streaming

**Script:** `optional_continuous.py`

Removed `availableNow=True` to use Spark's default recurring micro-batch trigger.

**Test:**
- Started Spark with an empty input directory
- Added the first ride file while the application was running
- Confirmed the anomaly appeared in the CSV output
- Added a second file containing the same ride
- Confirmed the anomaly count remained one
- Verified committed streaming batches 0, 1, and 2

**Result:** Spark processed newly arriving files without restarting and suppressed the duplicate ride.

## Isolation

All optional experiments use paths under `shared-folder/optional/`.

Generated input files, output files, Spark checkpoints, and SQL warehouse files are excluded from Git.

The original `main.py`, required model, and assignment outputs were not modified.

## Running

Use the existing Spark cluster and the shared directory:

```bash
docker cp optional_foreachbatch.py spark-master:/opt/spark/work-dir/
docker cp optional_dedup.py spark-master:/opt/spark/work-dir/
docker cp optional_continuous.py spark-master:/opt/spark/work-dir/
```

Run each script using `spark-submit` with the Spark master URL and shared-directory argument.

Use fresh optional checkpoints when replaying an entire input dataset. Do not remove required assignment checkpoints.

## Conclusion

All three optional demonstrations were implemented and tested successfully.
They are supplementary learning exercises and are not required for the assignment grade.
