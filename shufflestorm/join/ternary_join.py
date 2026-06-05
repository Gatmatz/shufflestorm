from pyspark.sql import Row


def run_ternary_join(A, B, C, b=10, c=10):
    """
    Execute A(x,y) ⋈ B(y,z) ⋈ C(z,w) as a single ternary join using the
    Afrati-Ullman shares (hypercube) algorithm.

    We use two hash functions to create b × c reducer groups:
      - h(y) = y mod b  →  partitions the y join-key into b buckets
      - g(z) = z mod c  →  partitions the z join-key into c buckets

    Each reducer is identified by a pair (i, j) where 0 ≤ i < b and 0 ≤ j < c.

    Replication rules (map phase):
      - A tuples: hashed by y into bucket i, then copied to all c groups
        (i, 0), (i, 1), …, (i, c-1).  Each A tuple produces c copies.
      - B tuples: hashed by both y and z, sent to exactly one group (h(y), g(z)).
        B is the only relation that is NOT replicated.
      - C tuples: hashed by z into bucket j, then copied to all b groups
        (0, j), (1, j), …, (b-1, j).  Each C tuple produces b copies.

    After the shuffle, each reducer (i, j) has everything it needs to
    compute its share of the final join locally.

    Args:
        A: Spark DataFrame with columns (x, y).
        B: Spark DataFrame with columns (y, z).
        C: Spark DataFrame with columns (z, w).
        b: Number of buckets for h (y-values). More buckets = finer partitioning of y.
        c: Number of buckets for g (z-values). More buckets = finer partitioning of z.

    Returns:
        RDD of Row(x, y, z, w) containing the join result.
    """
    # Hash functions: simple modulo to assign join keys to buckets
    def h(val):
        return int(val) % b

    def g(val):
        return int(val) % c

    # Convert DataFrames to RDDs so we can control the map/shuffle/reduce steps
    rdd_A = A.rdd
    rdd_B = B.rdd
    rdd_C = C.rdd

    # ===================== MAP PHASE =====================
    # Each function below takes a single row from its relation and
    # returns a list of (reducer_key, tagged_tuple) pairs.
    # The reducer_key (i, j) decides which reducer group receives the tuple.
    # The tag ("A"/"B"/"C") lets the reducer tell the relations apart.

    def map_A(row):
        # A only joins on y, so it is hashed by y into bucket i = h(y).
        # A does NOT use z, so it must be sent to every z-bucket j.
        # This means each A tuple is replicated c times: once for each j in [0, c).
        i = h(row.y)
        return [((i, j), ("A", row.x, row.y)) for j in range(c)]

    def map_B(row):
        # B joins on both y and z, so we can pin it to a single reducer
        # using both hash functions: (i, j) = (h(y), g(z)).
        # No replication needed — B goes to exactly one reducer per tuple.
        i = h(row.y)
        j = g(row.z)
        return [((i, j), ("B", row.y, row.z))]

    def map_C(row):
        # C only joins on z, so it is hashed by z into bucket j = g(z).
        # C does NOT use y, so it must be sent to every y-bucket i.
        # This means each C tuple is replicated b times: once for each i in [0, b).
        j = g(row.z)
        return [((i, j), ("C", row.z, row.w)) for i in range(b)]

    a_mapped = rdd_A.flatMap(map_A)
    b_mapped = rdd_B.flatMap(map_B)
    c_mapped = rdd_C.flatMap(map_C)

    # ===================== SHUFFLE PHASE =====================
    # Union all mapped tuples and group them by reducer key (i, j).
    # After this step, each group contains all the A, B, C tuples
    # that were assigned to that reducer.
    all_mapped = a_mapped.union(b_mapped).union(c_mapped)
    grouped = all_mapped.groupByKey()

    # ===================== REDUCE PHASE =====================
    # Each reducer (i, j) receives a mix of A, B, C tuples.
    # It separates them by tag, builds lookup indices on B and C,
    # then performs the local three-way join.
    def local_join(key_values):
        _, values = key_values

        # Separate the incoming tuples by their source relation
        a_tuples = []   # will hold (x, y) pairs from A
        b_tuples = []   # will hold (y, z) pairs from B
        c_tuples = []   # will hold (z, w) pairs from C

        for record in values:
            tag = record[0]
            if tag == "A":
                a_tuples.append((record[1], record[2]))
            elif tag == "B":
                b_tuples.append((record[1], record[2]))
            else:
                c_tuples.append((record[1], record[2]))

        # Index B tuples by y so we can quickly find matching z values
        b_by_y = {}
        for y_val, z_val in b_tuples:
            b_by_y.setdefault(y_val, []).append(z_val)

        # Index C tuples by z so we can quickly find matching w values
        c_by_z = {}
        for z_val, w_val in c_tuples:
            c_by_z.setdefault(z_val, []).append(w_val)

        # Three-way join: for each A tuple, find B matches on y,
        # then for each matching B tuple, find C matches on z.
        results = []
        for x_val, y_val in a_tuples:
            for z_val in b_by_y.get(y_val, []):
                for w_val in c_by_z.get(z_val, []):
                    results.append(Row(x=x_val, y=y_val, z=z_val, w=w_val))

        return results

    result_rdd = grouped.flatMap(local_join)

    return result_rdd
