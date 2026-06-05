import time
import json
from pathlib import Path
from shufflestorm.utils import get_spark_session, clear_spark_cache, get_spark_metrics
from shufflestorm.matchers.naive_matcher import run_naive_matching
from shufflestorm.matchers.group_matcher import run_group_matching
from shufflestorm.matchers.sql_matcher import run_sql_matching
from shufflestorm.matchers.afrati_matcher import run_afrati_matching
from shufflestorm.config import configurations
from shufflestorm.utils import spark_ui

def main():
    # Use all available CPU cores (local[*]) with adaptive query execution
    # so Spark can dynamically coalesce shuffle partitions at runtime.
    spark = get_spark_session(
        app_name="Distributed Systems", 
        master="local[*]", 
        config={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.shuffle.partitions": str(configurations.NUMBER_OF_REDUCERS),
            "spark.default.parallelism": str(configurations.NUMBER_OF_REDUCERS)
            }
        )

    # Load the dataset once; we derive both a DataFrame (for SQL) and an RDD
    # (for the manual matching strategies) from the same source.
    dataframe = spark.read.csv(f"data/{configurations.DATASET}.csv", header=True, inferSchema=True)
    data_rdd = dataframe.rdd

    # Record the experiment parameters alongside the results so each output
    # file is fully self-describing and reproducible.
    results = {
        "settings": {
            "DATASET": configurations.DATASET,
            "DATASET_SIZE": configurations.DATASET_SIZE,
            "REDUCER_SIZE": configurations.REDUCER_SIZE,
            "P_PRIME": configurations.P_PRIME,
        },
        "results": {}
    }

    # =================== 1. Test Naïve Strategy ===================
    # The naïve approach sends every record to a single reducer, which then
    # compares all possible pairs. Simple but creates a massive shuffle.
    #
    # Benchmarking methodology used for every strategy below:
    #   - Sleep before/after to let Spark's internal metric counters settle,
    #     since they update asynchronously via the listener bus.
    #   - Snapshot metrics (task count, shuffle read/write bytes) before and
    #     after, then diff to isolate this strategy's contribution.
    #   - Time only the .count() action, which forces Spark to materialise
    #     the lazy RDD and gives us wall-clock execution time.
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    naive_res = run_naive_matching(data_rdd=data_rdd)
    start = time.time()
    naive_count = naive_res.count()
    naive_time = time.time() - start
    time.sleep(1)
    
    # Shuffle volume = shuffle bytes written + shuffle bytes read, giving
    # the total amount of data moved across the network/disk for this job.
    metrics_after = get_spark_metrics(spark)
    naive_tasks = metrics_after[0] - metrics_before[0]
    naive_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    results["results"]["naive"] = {
        "pairs_found": naive_count, 
        "time": naive_time,
        "total_tasks": naive_tasks,
        "shuffle_volume_bytes": naive_shuffle
    }
    print(f"Naïve Approach Finished. Pairs Found: {naive_count} | Time: {naive_time:.4f}s | Tasks: {naive_tasks} | Shuffle Bytes: {naive_shuffle}")

    # Unpersist cached data and clear the RDD lineage so the next strategy
    # starts cold — otherwise Spark could reuse cached partitions, making
    # its shuffle/time numbers artificially low.
    clear_spark_cache(spark, data_rdd)

    # =================== 2. Test Group-based Strategy ===================
    # Splits the dataset into fixed-size groups and pairs groups so that
    # each reducer handles at most REDUCER_SIZE rows. This distributes work
    # more evenly than naïve but still duplicates data across groups.
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    group_res = run_group_matching(data_rdd=data_rdd, total_rows=configurations.DATASET_SIZE, group_size=configurations.REDUCER_SIZE)
    start = time.time()
    group_count = group_res.count()
    group_time = time.time() - start
    time.sleep(1)

    metrics_after = get_spark_metrics(spark)
    group_tasks = metrics_after[0] - metrics_before[0]
    group_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    results["results"]["group"] = {
        "pairs_found": group_count, 
        "time": group_time,
        "total_tasks": group_tasks,
        "shuffle_volume_bytes": group_shuffle
    }
    print(f"Group-Based Approach Finished. Pairs Found: {group_count} | Time: {group_time:.4f}s | Tasks: {group_tasks} | Shuffle Bytes: {group_shuffle}")
    
    clear_spark_cache(spark, data_rdd)

    # =================== 3. Test Spark SQL & Optimizer ===================
    # Expresses the all-pairs problem as a SQL self-join and lets Spark's
    # Catalyst optimizer choose the physical plan (e.g., broadcast hash join
    # vs. sort-merge join). This tests how well built-in optimisations
    # compete with our hand-rolled RDD strategies.
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    sql_res = run_sql_matching(spark=spark, dataframe=dataframe, explain=False)
    start = time.time()
    sql_count = sql_res.count()
    sql_time = time.time() - start
    time.sleep(1)
    metrics_after = get_spark_metrics(spark)
    sql_tasks = metrics_after[0] - metrics_before[0]
    sql_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])
    results["results"]["sql"] = {
        "pairs_found": sql_count, 
        "time": sql_time,
        "total_tasks": sql_tasks,
        "shuffle_volume_bytes": sql_shuffle
    }
    print(f"Spark SQL Approach Finished. Pairs Found: {sql_count} | Time: {sql_time:.4f}s | Tasks: {sql_tasks} | Shuffle Bytes: {sql_shuffle}")
    
    clear_spark_cache(spark, data_rdd)
    
    # =================== 4. Test Afrati-Ullmann Strategy ===================
    # Uses the theoretical framework from Afrati & Ullman to hash-partition
    # rows into p^2 reducer buckets (p = P_PRIME). This minimises the
    # maximum replication rate, achieving near-optimal communication cost
    # at the expense of a more complex partitioning step.
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    afrati_res = run_afrati_matching(data_rdd=data_rdd, p = configurations.P_PRIME, d = configurations.DATASET_SIZE)
    start = time.time()
    afrati_count = afrati_res.count()
    afrati_time = time.time() - start
    time.sleep(1)

    metrics_after = get_spark_metrics(spark)
    afrati_tasks = metrics_after[0] - metrics_before[0]
    afrati_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    results["results"]["afrati"] = {
        "pairs_found": afrati_count, 
        "time": afrati_time,
        "total_tasks": afrati_tasks,
        "shuffle_volume_bytes": afrati_shuffle
    }
    print(f"Afrati-Ullmann Approach Finished. Pairs Found: {afrati_count} | Time: {afrati_time:.4f}s | Tasks: {afrati_tasks} | Shuffle Bytes: {afrati_shuffle}")

    # Persist all results to a JSON file named after the dataset so we can
    # compare runs across different dataset sizes or configurations later.
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    results_file = results_dir / f"{configurations.DATASET}_{configurations.REDUCER_SIZE}reducer_results.json"
    
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {results_file}")
    
    # spark_ui()  # Keep Spark session alive to check metrics in UI before stopping

    spark.stop()

if __name__ == "__main__":
    main()