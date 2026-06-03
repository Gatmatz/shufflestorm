"""
Entry point for Part 2 — Triple Join Benchmark.

Computes A(x, y) ⋈ B(y, z) ⋈ C(z, w) using three strategies:
  1. Direct ternary join
  2. Two consecutive binary joins
  3. Spark SQL query

All relations are generated with the same configurable size.
Results (row counts, execution times, task counts, shuffle volumes)
are printed and saved to results/<RELATION_SIZE>_triplejoin_results.json.
"""

import time
import json
from pathlib import Path
from shufflestorm.utils import get_spark_session, clear_spark_cache, get_spark_metrics
from shufflestorm.data_generator import generate_relations
from shufflestorm.ternary_join import run_ternary_join
from shufflestorm.binary_join import run_binary_joins
from shufflestorm.sql_join import run_sql_join


# ==========================================
# Configuration — set the relation size here
# ==========================================
RELATION_SIZE = 1000  # Number of rows for each of A, B, C


def main():
    # Initialize Spark session
    spark = get_spark_session(
        app_name="Triple Join Benchmark",
        master="local[*]",
        config={"spark.sql.adaptive.enabled": "true"},
    )

    # Generate relations A(x,y), B(y,z), C(z,w)
    A, B, C = generate_relations(spark, size=RELATION_SIZE)

    results = {
        "settings": {"RELATION_SIZE": RELATION_SIZE},
        "results": {},
    }

    # =================== 1. Direct Ternary Join ===================
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    ternary_res = run_ternary_join(A, B, C)
    start = time.time()
    ternary_count = ternary_res.count()
    ternary_time = time.time() - start
    time.sleep(1)

    metrics_after = get_spark_metrics(spark)
    ternary_tasks = metrics_after[0] - metrics_before[0]
    ternary_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    results["results"]["ternary"] = {
        "rows": ternary_count,
        "time": ternary_time,
        "total_tasks": ternary_tasks,
        "shuffle_volume_bytes": ternary_shuffle,
    }
    print(f"Ternary Join Finished. Rows: {ternary_count} | Time: {ternary_time:.4f}s | Tasks: {ternary_tasks} | Shuffle Bytes: {ternary_shuffle}")

    clear_spark_cache(spark, A.rdd)

    # =================== 2. Two Consecutive Binary Joins ===================
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

    # =================== 3. Spark SQL Join ===================
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

    # Save results
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    results_file = results_dir / f"{RELATION_SIZE}_triplejoin_results.json"

    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to {results_file}")

    spark.stop()


if __name__ == "__main__":
    main()
