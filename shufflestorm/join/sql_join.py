def run_sql_join(spark, A, B, C, explain=False):
    """
    Compute the three-way join A ⋈ B ⋈ C by writing plain SQL and letting
    Spark handle everything else.

    Unlike the manual RDD-based join approaches (where we explicitly control
    partitioning, shuffle boundaries, and join order), this method delegates
    all optimization to Spark's Catalyst optimizer. Catalyst will analyze table
    statistics, choose join order, pick join strategies (broadcast vs. shuffle
    hash vs. sort-merge), and insert exchanges (shuffles) automatically.

    Args:
        spark: Active SparkSession.
        A: Spark DataFrame with columns (x, y).
        B: Spark DataFrame with columns (y, z).
        C: Spark DataFrame with columns (z, w).
        explain: If True, print the extended physical plan so we can see
                 exactly what Catalyst decided to do.

    Returns:
        Spark DataFrame with columns (x, y, z, w) — the full join result.
    """

    # Expose each DataFrame as a named table so the SQL engine can reference
    # them. These disappear when the SparkSession ends or when overwritten by another call.
    A.createOrReplaceTempView("A")
    B.createOrReplaceTempView("B")
    C.createOrReplaceTempView("C")

    # We just describe *what* we want (the two equi-joins); Catalyst figures
    # out the best *how* — join order, algorithm, parallelism, and whether
    # any table is small enough to broadcast.
    result = spark.sql("""
        SELECT A.x, A.y, B.z, C.w
        FROM A JOIN B ON A.y = B.y
        JOIN C ON B.z = C.z
    """)

    # Dump the full plan (parsed, analyzed, optimized, physical) so we
    # can compare Catalyst's choices against our manual RDD strategies.
    if explain:
        print("=" * 80)
        print("SQL JOIN - PHYSICAL EXECUTION PLAN")
        print("=" * 80)
        result.explain(mode="extended")
        print("=" * 80)

    return result

