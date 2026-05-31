import time
import json
from pathlib import Path
from shufflestorm.utils import get_spark_session, clear_spark_cache
from shufflestorm.naive_matcher import run_naive_matching
from shufflestorm.group_matcher import run_group_matching
from shufflestorm.sql_matcher import run_sql_matching
from shufflestorm.afrati_matcher import run_afrati_matching
from shufflestorm.config import settings

def main():
    # Initialize Spark Session locally using all available cores
    spark = get_spark_session(
        app_name="Distributed Systems M", 
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

    # Test Naïve Strategy
    naive_res = run_naive_matching(data_rdd=data_rdd)
    start = time.time()
    naive_count = naive_res.count()
    naive_time = time.time() - start
    results["results"]["naive"] = {"pairs_found": naive_count, "time": naive_time}
    print(f"Naïve Approach Finished. Pairs Found: {naive_count} | Time: {naive_time:.4f}s")

    clear_spark_cache(spark, data_rdd)

    # Test Group-based Strategy
    group_res = run_group_matching(data_rdd=data_rdd, total_rows=settings.DATASET_SIZE, group_size=settings.REDUCER_SIZE)
    start = time.time()
    group_count = group_res.count()
    group_time = time.time() - start
    results["results"]["group"] = {"pairs_found": group_count, "time": group_time}
    print(f"Group-Based Approach Finished. Pairs Found: {group_count} | Time: {group_time:.4f}s")
    
    clear_spark_cache(spark, data_rdd)

    # Test Spark SQL & Optimizer
    sql_res = run_sql_matching(spark=spark, dataframe=dataframe, explain=False)
    start = time.time()
    sql_count = sql_res.count()
    sql_time = time.time() - start
    results["results"]["sql"] = {"pairs_found": sql_count, "time": sql_time}
    print(f"Spark SQL Approach Finished. Pairs Found: {sql_count} | Time: {sql_time:.4f}s")
    
    clear_spark_cache(spark, data_rdd)
    
    # Test Afrati-Ullmann Strategy
    afrati_res = run_afrati_matching(data_rdd=data_rdd, p = settings.P_PRIME, d = settings.DATASET_SIZE)
    start = time.time()
    afrati_count = afrati_res.count()
    afrati_time = time.time() - start
    results["results"]["afrati"] = {"pairs_found": afrati_count, "time": afrati_time}
    print(f"Afrati-Ullmann Approach Finished. Pairs Found: {afrati_count} | Time: {afrati_time:.4f}s")

    # Save results to file
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    results_file = results_dir / f"{settings.DATASET}_results.json"
    
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {results_file}")
    
    spark.stop()

if __name__ == "__main__":
    main()