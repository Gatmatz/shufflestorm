import time
import json
from pathlib import Path
from shufflestorm.utils import get_spark_session, clear_spark_cache, get_spark_metrics
from shufflestorm.naive_matcher import run_naive_matching
from shufflestorm.group_matcher import run_group_matching
from shufflestorm.sql_matcher import run_sql_matching
from shufflestorm.afrati_matcher import run_afrati_matching
from shufflestorm.config import settings
from shufflestorm.utils import spark_ui

def main():
    # Initialize Spark Session locally using all available cores
    spark = get_spark_session(
        app_name="Distributed Systems", 
        master="local[*]", 
        config={"spark.sql.adaptive.enabled": "true"}
        )

    # Dataset loading
    dataframe = spark.read.csv(f"data/{settings.DATASET}.csv", header=True, inferSchema=True)
    data_rdd = dataframe.rdd

    # Dictionary to store results with settings
    results = {
        "settings": {
            "DATASET": settings.DATASET,
            "DATASET_SIZE": settings.DATASET_SIZE,
            "REDUCER_SIZE": settings.REDUCER_SIZE,
            "P_PRIME": settings.P_PRIME,
        },
        "results": {}
    }

    # =================== 1. Test Naïve Strategy ===================
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    naive_res = run_naive_matching(data_rdd=data_rdd)
    start = time.time()
    naive_count = naive_res.count()
    naive_time = time.time() - start
    time.sleep(1)
    
    # Get metrics after execution to calculate differences
    metrics_after = get_spark_metrics(spark)
    naive_tasks = metrics_after[0] - metrics_before[0]
    naive_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    # Store results for Naïve approach in the results dictionary
    results["results"]["naive"] = {
        "pairs_found": naive_count, 
        "time": naive_time,
        "total_tasks": naive_tasks,
        "shuffle_volume_bytes": naive_shuffle
    }
    print(f"Naïve Approach Finished. Pairs Found: {naive_count} | Time: {naive_time:.4f}s | Tasks: {naive_tasks} | Shuffle Bytes: {naive_shuffle}")

    # Clear Spark cache before next test to ensure fair comparison
    clear_spark_cache(spark, data_rdd)

    # =================== 2. Test Group-based Strategy ===================
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    group_res = run_group_matching(data_rdd=data_rdd, total_rows=settings.DATASET_SIZE, group_size=settings.REDUCER_SIZE)
    start = time.time()
    group_count = group_res.count()
    group_time = time.time() - start
    time.sleep(1)

    # Get metrics after execution to calculate differences
    metrics_after = get_spark_metrics(spark)
    group_tasks = metrics_after[0] - metrics_before[0]
    group_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    # Store results for Group-based approach in the results dictionary
    results["results"]["group"] = {
        "pairs_found": group_count, 
        "time": group_time,
        "total_tasks": group_tasks,
        "shuffle_volume_bytes": group_shuffle
    }
    print(f"Group-Based Approach Finished. Pairs Found: {group_count} | Time: {group_time:.4f}s | Tasks: {group_tasks} | Shuffle Bytes: {group_shuffle}")
    
    # Clear Spark cache before next test to ensure fair comparison
    clear_spark_cache(spark, data_rdd)

    # =================== 3. Test Spark SQL & Optimizer ===================
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
    time.sleep(1)
    metrics_before = get_spark_metrics(spark)
    afrati_res = run_afrati_matching(data_rdd=data_rdd, p = settings.P_PRIME, d = settings.DATASET_SIZE)
    start = time.time()
    afrati_count = afrati_res.count()
    afrati_time = time.time() - start
    time.sleep(1)

    # Get metrics after execution to calculate differences
    metrics_after = get_spark_metrics(spark)
    afrati_tasks = metrics_after[0] - metrics_before[0]
    afrati_shuffle = (metrics_after[1] - metrics_before[1]) + (metrics_after[2] - metrics_before[2])

    # Store results for Afrati-Ullmann approach in the results dictionary
    results["results"]["afrati"] = {
        "pairs_found": afrati_count, 
        "time": afrati_time,
        "total_tasks": afrati_tasks,
        "shuffle_volume_bytes": afrati_shuffle
    }
    print(f"Afrati-Ullmann Approach Finished. Pairs Found: {afrati_count} | Time: {afrati_time:.4f}s | Tasks: {afrati_tasks} | Shuffle Bytes: {afrati_shuffle}")

    # Save overall results dictionary to file
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    results_file = results_dir / f"{settings.DATASET}_results.json"
    
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {results_file}")
    
    # spark_ui()  # Keep Spark session alive to check metrics in UI before stopping

    spark.stop()

if __name__ == "__main__":
    main()