from pyspark.sql import SparkSession, DataFrame
import time

def run_sql_matching(spark: SparkSession, dataframe: DataFrame, explain: bool = True) -> DataFrame:
    """
    SQL-based matching solution that finds names with exact matches between datasets.
    
    Args:
        spark: SparkSession object
        explain: If True, prints the physical execution plan
    
    Returns:
        DataFrame containing matching names from both datasets
    """

    # Create temporary SQL views
    dataframe.createOrReplaceTempView("datasets")

    # Execute SQL query to find exact matches
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
        AND d1.name = d2.name
    """
    
    result = spark.sql(query)

    # Print execution plan if requested
    if explain:
        print("=" * 80)
        print("SQL MATCHER - PHYSICAL EXECUTION PLAN")
        print("=" * 80)
        result.explain(mode="extended")
        print("=" * 80)
    
    return result