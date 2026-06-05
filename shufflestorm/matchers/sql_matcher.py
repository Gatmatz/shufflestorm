from pyspark.sql import SparkSession, DataFrame
import time

def run_sql_matching(spark: SparkSession, dataframe: DataFrame, explain: bool = True) -> DataFrame:
    """
    SQL-based matching that delegates join strategy entirely to Spark's
    Catalyst optimizer.

    Instead of manually building RDD pipelines, this approach expresses the
    all-pairs matching problem as a SQL query and lets Catalyst decide the
    best physical execution plan (broadcast hash join, sort-merge join, etc.)
    based on data statistics and cluster configuration.

    Args:
        spark: Active SparkSession.
        dataframe: Input DataFrame with 'id' and 'name' columns.
        explain: If True, prints Catalyst's chosen execution plan so you can
                 see what physical operators (join type, exchange strategy,
                 filters) Spark actually selected.

    Returns:
        DataFrame of matched pairs with columns from both sides.
    """

    # Register the DataFrame as a SQL-queryable temporary view
    dataframe.createOrReplaceTempView("datasets")

    # CROSS JOIN + WHERE is the all-pairs comparison: every unique ordered
    # pair of records is produced. Without an equality predicate on a data
    # column, Catalyst cannot rewrite this as an equi-join, so the physical
    # plan executes a true Cartesian product. The d1.id < d2.id condition
    # removes self-matches and mirror duplicates.
    query = """
    SELECT 
        d1.id AS rdd1_id, 
        d1.name AS dataset1_name, 
        d2.id AS rdd2_id, 
        d2.name AS dataset2_name
    FROM 
        datasets d1
    CROSS JOIN 
        datasets d2
    WHERE 
        d1.id < d2.id
    """
    
    result = spark.sql(query)

    # The extended execution plan shows four levels:
    #   - Parsed logical plan  (raw SQL → tree)
    #   - Analyzed logical plan (resolved column names and types)
    #   - Optimized logical plan (Catalyst rule-based rewrites)
    #   - Physical plan (actual operators Spark will execute)
    if explain:
        print("=" * 80)
        print("SQL MATCHER - PHYSICAL EXECUTION PLAN")
        print("=" * 80)
        result.explain(mode="extended")
        print("=" * 80)
    
    return result