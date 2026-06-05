"""
Benchmark comparing three ways to compute a triple join: A(x,y) ⋈ B(y,z) ⋈ C(z,w).

The goal is to measure how different join strategies perform on the same data,
so we can understand the trade-offs in execution time, parallelism (task count),
and network cost (shuffle volume). The three strategies are:

  1. Ternary join   – joins all three relations in a single logical step.
  2. Binary joins   – chains two pairwise joins (A⋈B, then result⋈C), which is
                      the conventional approach but may produce a large intermediate.
  3. Spark SQL join – expresses the same logic as a SQL query, letting the Catalyst
                      optimizer choose its own physical plan independently.

For each strategy we capture four metrics:
  - Row count      : correctness check — all strategies must produce the same count.
  - Wall-clock time: how long the materialization (count action) takes.
  - Task count     : total Spark tasks spawned, reflecting parallelism overhead.
  - Shuffle volume : bytes read + written during shuffles, reflecting network/IO cost.

Results are written to results/<RELATION_SIZE>_triplejoin_results.json so they
can be compared across runs with different relation sizes.
"""

import time
import json
from pathlib import Path
from shufflestorm.config import configurations
from shufflestorm.utils import get_spark_session, clear_spark_cache, get_spark_metrics
from shufflestorm.preprocessing.synthetic_data_generator import generate_relations
from shufflestorm.join.ternary_join import run_ternary_join
from shufflestorm.join.binary_join import run_binary_joins
from shufflestorm.join.sql_join import run_sql_join
from shufflestorm.config.configurations import RELATION_SIZE, B as B_buckets, C as C_buckets


def main():
    # Use all available cores ("local[*]") and enable Adaptive Query Execution
    # so Spark can dynamically coalesce partitions.
    spark = get_spark_session(
        app_name="Triple Join Benchmark",
        master="local[*]",
        config={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.shuffle.partitions": str(configurations.NUMBER_OF_REDUCERS),
            "spark.default.parallelism": str(configurations.NUMBER_OF_REDUCERS)
            },
    )

    # Build three synthetic relations that share join keys:
    #   A(x, y), B(y, z), C(z, w)
    # Each is returned as a DataFrame.
    A, B, C = generate_relations(spark, size=RELATION_SIZE)

    results = {
        "settings": {
            "RELATION_SIZE": RELATION_SIZE, 
            "B": B_buckets, 
            "C": C_buckets, 
            "NUMBER_OF_REDUCERS": configurations.NUMBER_OF_REDUCERS
            },
        "results": {},
    }

    # --- Strategy 1: Ternary Join ---
    # Joins A, B, and C in one logical step. 
    
    # We sleep before and after the measured section so that any lingering
    # background tasks (e.g. shuffle cleanup, metric reporting) have time
    # to finish. This makes the before/after metric snapshots more accurate.
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    ternary_res = run_ternary_join(A, B, C, b=B_buckets, c=C_buckets)
    start = time.time()
    ternary_count = ternary_res.count()
    ternary_time = time.time() - start
    time.sleep(1)

    metrics_after = get_spark_metrics(spark)
    ternary_tasks = metrics_after[0] - metrics_before[0]
    # Shuffle volume = bytes written + bytes read across all shuffle stages.
    ternary_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    results["results"]["ternary"] = {
        "rows": ternary_count,
        "time": ternary_time,
        "total_tasks": ternary_tasks,
        "shuffle_volume_bytes": ternary_shuffle
    }
    print(f"Ternary Join Finished. Rows: {ternary_count} | Time: {ternary_time:.4f}s | Tasks: {ternary_tasks} | Shuffle Bytes: {ternary_shuffle}")

    # Clear cached data and shuffle files between strategies so that one
    # run's residual state doesn't give the next run an unfair advantage
    # (e.g. warm OS page cache, pre-computed shuffle blocks).
    clear_spark_cache(spark, A.rdd)

    # --- Strategy 2: Two Consecutive Binary Joins ---
    # First joins A⋈B on column y, then joins the intermediate result with C
    # on column z. This is the most common real-world pattern but can produce
    # a large intermediate if the first join has high fan-out.
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    binary_res = run_binary_joins(A, B, C)
    start = time.time()
    binary_count = binary_res.count()
    binary_time = time.time() - start
    time.sleep(1)

    metrics_after = get_spark_metrics(spark)
    binary_tasks = metrics_after[0] - metrics_before[0]
    binary_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    results["results"]["binary"] = {
        "rows": binary_count,
        "time": binary_time,
        "total_tasks": binary_tasks,
        "shuffle_volume_bytes": binary_shuffle,
    }
    print(f"Binary Joins Finished. Rows: {binary_count} | Time: {binary_time:.4f}s | Tasks: {binary_tasks} | Shuffle Bytes: {binary_shuffle}")

    clear_spark_cache(spark, A.rdd)

    # --- Strategy 3: Spark SQL Join ---
    # Registers the DataFrames as temporary views and runs an equivalent
    # SQL query. The Catalyst optimizer takes place.
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    sql_res = run_sql_join(spark, A, B, C)
    start = time.time()
    sql_count = sql_res.count()
    sql_time = time.time() - start
    time.sleep(1)

    metrics_after = get_spark_metrics(spark)
    sql_tasks = metrics_after[0] - metrics_before[0]
    sql_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    results["results"]["sql"] = {
        "rows": sql_count,
        "time": sql_time,
        "total_tasks": sql_tasks,
        "shuffle_volume_bytes": sql_shuffle,
    }
    print(f"SQL Join Finished. Rows: {sql_count} | Time: {sql_time:.4f}s | Tasks: {sql_tasks} | Shuffle Bytes: {sql_shuffle}")

    # Persist all metrics to a JSON file keyed by relation size, making it
    # easy to compare results across multiple runs with different sizes.
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    results_file = results_dir / f"{RELATION_SIZE}_{configurations.NUMBER_OF_REDUCERS}_triplejoin_results.json"

    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to {results_file}")

    spark.stop()


if __name__ == "__main__":
    main()

