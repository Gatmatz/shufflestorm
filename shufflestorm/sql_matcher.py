from pyspark.sql import SparkSession, DataFrame
import time

def run_sql_matching(spark: SparkSession, dataframe: DataFrame, explain: bool = True) -> DataFrame:
    """
    SQL-based matching solution that finds bike names with exact matches between datasets.
    
    Args:
        spark: SparkSession object
        explain: If True, prints the physical execution plan
    
    Returns:
        DataFrame containing matching bike names from both datasets
    """

    # Create temporary SQL views
    dataframe.createOrReplaceTempView("bikes")

    # Execute SQL query to find exact matches
    query = """
    SELECT 
        b1.id AS rdd1_id, 
        b1.name AS bike1_name, 
        b2.id AS rdd2_id, 
        b2.name AS bike2_name
    FROM 
        bikes b1
    CROSS JOIN 
        bikes b2
    WHERE 
        b1.id < b2.id 
        AND b1.name = b2.name
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