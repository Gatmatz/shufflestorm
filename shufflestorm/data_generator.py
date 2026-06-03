"""
Generate random relations A(x, y), B(y, z), and C(z, w) as Spark DataFrames.

All three relations share the same configurable size so that the join
strategies can be benchmarked under identical conditions.
"""


def generate_relations(spark, size):
    """
    Create three DataFrames A(x, y), B(y, z), and C(z, w) of the given size.

    Args:
        spark: Active SparkSession.
        size:  Number of rows to generate for each relation.

    Returns:
        Tuple of (A, B, C) Spark DataFrames.
    """
    # TODO: Implement relation generation
    raise NotImplementedError("generate_relations is not yet implemented")
