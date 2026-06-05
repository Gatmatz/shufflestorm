"""
Synthetic data generator for three-way join benchmarks.

Produces relations A(x, y), B(y, z), and C(z, w) that are designed to
join cleanly with each other.  Every relation has the same number of rows
so benchmark comparisons between join strategies are fair.  The join-key
domains (y, z) deliberately overlap across relations — without that
overlap the joins would return empty results and the benchmarks would
be meaningless.
"""

from pyspark.sql.functions import col


def generate_relations(spark, size):
    """
    Build three equally-sized DataFrames whose key columns are guaranteed
    to overlap, producing a meaningful number of matched rows when joined
    as A ⋈ B ⋈ C.

    Args:
        spark: Active SparkSession.
        size:  Number of rows in each relation.  Larger values stress
               shuffle and network transfer; smaller values are useful
               for quick smoke tests.

    Returns:
        (A, B, C) — a tuple of Spark DataFrames ready to be joined on
        their shared key columns (y between A and B, z between B and C).
    """

    # Relation A — x is a unique row identifier, y is derived via
    # modulo 1000 so it cycles through values 0–999.  This creates
    # roughly (size / 1000) duplicate y-values per group, which gives
    # the join with B plenty of matching rows to work with.
    A = (
        spark.range(size)
        .withColumn("y", col("id") % 1000)
        .selectExpr("id as x", "y")
    )

    # Relation B — y is computed with the same "id % 1000" formula used
    # in A, so the two relations share the exact same key domain (0–999).
    # z is simply the row id shifted by 10; the shift is arbitrary but
    # keeps z values distinct from the raw ids so bugs that accidentally
    # compare the wrong columns are easier to spot.
    B = (
        spark.range(size)
        .withColumn("z", col("id") + 10)
        .selectExpr("id % 1000 as y", "z")
    )

    # Relation C — z uses "id % 1000" to mirror the domain of B's z
    # after the modulo (both range over 0–999), ensuring the B ⋈ C join
    # also produces matches.  w is shifted by 100 for the same
    # distinguishability reason as B's z offset.
    C = (
        spark.range(size)
        .withColumn("w", col("id") + 100)
        .selectExpr("id % 1000 as z", "w")
    )

    return A, B, C
