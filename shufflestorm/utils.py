import json
from pyspark.sql import SparkSession
from pyspark import RDD
import urllib.request
from pathlib import Path

def get_spark_session(app_name: str = "spark-app", master: str = "local[*]", config: dict = None) -> SparkSession:
    """
    Build a SparkSession with optional custom configuration.

    We centralise session creation here so that every benchmark run uses
    the same defaults (app name, master URL) while still allowing
    per-experiment overrides such as shuffle partition counts or memory
    limits via the ``config`` dict.

    Args:
        app_name: Human-readable label shown in the Spark UI.
        master: Cluster URL. Use "local[*]" for local testing with all
                available cores, or a YARN / standalone URL for cluster runs.
        config: Extra Spark config entries (e.g.
                {"spark.sql.shuffle.partitions": "8"}).  Defaults to an
                empty dict so callers don't need to pass one explicitly.

    Returns:
        A ready-to-use SparkSession (reuses an existing one if available).
    """
    if config is None:
        config = {}
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Inject any experiment-specific Spark settings (e.g. partition count,
    # memory caps) so each benchmark scenario is fully reproducible.
    for key, value in config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()

def clear_spark_cache(spark: SparkSession, data_rdd: RDD):
    """
    Evict all cached data so the next run starts with a clean slate.

    Between benchmark iterations we need to drop cached DataFrames and
    unpersist the source RDD.  Without this, a subsequent strategy would
    read from memory instead of recomputing, which would skew shuffle
    metrics and make comparisons unfair.

    Args:
        spark: Active session whose catalog cache will be cleared.
        data_rdd: The RDD that was persisted for the previous run;
                  calling unpersist releases its memory blocks.
    """
    spark.catalog.clearCache()
    data_rdd.unpersist()
    
def get_spark_metrics(spark):
    """
    Query the Spark UI REST API to collect task and shuffle statistics.

    We pull metrics directly from the REST API (rather than parsing logs)
    because it gives us structured, per-stage numbers for task counts,
    shuffle read bytes, and shuffle write bytes.  These three values are
    the primary indicators we use to compare partitioning / shuffle
    strategies in our benchmarks.

    Returns a tuple of (total_tasks, total_shuffle_read_bytes,
    total_shuffle_write_bytes).  On any error (e.g. the UI is disabled)
    returns (0, 0, 0) so callers can still run without crashing.
    """
    try:
        # Build the stages endpoint URL from the live Spark context.
        ui_url = spark.sparkContext.uiWebUrl
        app_id = spark.sparkContext.applicationId
        url = f"{ui_url}/api/v1/applications/{app_id}/stages"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            stages = json.loads(response.read().decode())
        
        total_tasks = 0
        total_shuffle_read = 0
        total_shuffle_write = 0
        
        # Accumulate across all stages to get application-wide totals.
        for stage in stages:
            total_tasks += stage.get('numTasks', 0)
            total_shuffle_read += stage.get('shuffleReadBytes', 0)
            total_shuffle_write += stage.get('shuffleWriteBytes', 0)
            
        return total_tasks, total_shuffle_read, total_shuffle_write
    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return 0, 0, 0

def spark_ui():
    """
    Block until the user presses Enter, keeping the Spark UI alive.

    After a job finishes Spark tears down its web UI.  Calling this at
    the end of a script lets you open http://localhost:4040 to inspect
    stages, DAGs, and shuffle details before the process exits.
    """
    input("Press Enter to exit the script...")