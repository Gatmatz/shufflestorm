import json
from pyspark.sql import SparkSession
from pyspark import RDD
import urllib.request
from pathlib import Path

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
    
def get_spark_metrics(spark):
    """Fetches and computes Spark execution metrics (tasks, shuffle read/write bytes) from the Spark UI REST API."""
    try:
        ui_url = spark.sparkContext.uiWebUrl
        app_id = spark.sparkContext.applicationId
        url = f"{ui_url}/api/v1/applications/{app_id}/stages"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            stages = json.loads(response.read().decode())
        
        total_tasks = 0
        total_shuffle_read = 0
        total_shuffle_write = 0
        
        for stage in stages:
            total_tasks += stage.get('numTasks', 0)
            total_shuffle_read += stage.get('shuffleReadBytes', 0)
            total_shuffle_write += stage.get('shuffleWriteBytes', 0)
            
        return total_tasks, total_shuffle_read, total_shuffle_write
    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return 0, 0, 0

def spark_ui():
    """Persist script execution to check Spark UI metrics"""
    input("Press Enter to exit the script...")