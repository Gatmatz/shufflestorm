"""Shared utilities for Spark jobs."""

from pyspark.sql import SparkSession
from pyspark import RDD

def get_spark_session(app_name: str = "spark-app", master: str = "local[*]", config: dict = None) -> SparkSession:
    """
    Create and return a Spark session.
    
    Args:
        app_name: Name of the Spark application
        master: Master URL (local[*], yarn, etc.)
        config: Dictionary of Spark configuration key-value pairs
        
    Returns:
        SparkSession object
    """
    if config is None:
        config = {}
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Apply all configs
    for key, value in config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()

def clear_spark_cache(spark: SparkSession, data_rdd: RDD):
    """
    Clear Spark's cache to free up memory.
    
    Args:
        spark: SparkSession object
        data_rdd: RDD object to unpersist
    """
    spark.catalog.clearCache()
    data_rdd.unpersist()
