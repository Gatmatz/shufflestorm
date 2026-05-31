from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, levenshtein
import time 

def run_naive_matching(data_rdd: DataFrame) -> DataFrame:
    """
    Naive matching solution using Spark that checks each row with each other row.
    We use the cartesian product to generate all possible pairs of records and then filter them based on the matching criteria.
    """
    naive_pairs_rdd = data_rdd.cartesian(data_rdd)

    # Assuming each record is a tuple like (id, data)
    # To avoid duplicates (x, y) and (y, x), and self-matches (x, x), we filter the id:    
    # To return only pairs where the name is the same, we also filter by the data.
    unique_naive_pairs = naive_pairs_rdd.filter(lambda x: x[0][0] < x[1][0] and x[0][1] == x[1][1])

    return unique_naive_pairs