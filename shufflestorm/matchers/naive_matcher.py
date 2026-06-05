from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, levenshtein
import time 

def run_naive_matching(data_rdd: DataFrame) -> DataFrame:
    """
    Brute-force matching using a cartesian (cross) product.

    Every record is paired with every other record, producing O(n²) pairs.
    This is the simplest approach but the most expensive — it sends all
    data to all executors and scales quadratically with the dataset size.
    """
    naive_pairs_rdd = data_rdd.cartesian(data_rdd)

    # Filter the O(n²) pairs down to only the ones we care about:
    #   x[0][0] < x[1][0]  — keeps only the pair where the smaller id
    #                         comes first. This single condition eliminates
    #                         both self-matches (where ids are equal) and
    #                         mirror duplicates like (A,B) vs (B,A).
    unique_naive_pairs = naive_pairs_rdd.filter(lambda x: x[0][0] < x[1][0])

    return unique_naive_pairs