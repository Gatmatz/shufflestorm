from pyspark.sql import Row


def _hash_join(left_rdd, left_key_fn, right_rdd, right_key_fn, combine_fn):
    """
    Generic hash-based equi-join between two RDDs.

    This is the building block for the binary join strategy. It works in
    three stages: key extraction (map), shuffling rows with the same key
    to the same partition (cogroup), and producing matched pairs locally.

    We use cogroup instead of join because it gives us explicit control
    over how left and right rows are paired that proves to be useful for building custom
    output tuples via combine_fn.

    Args:
        left_rdd:      RDD representing the left relation.
        left_key_fn:   Extracts the join key from a left-side tuple.
        right_rdd:     RDD representing the right relation.
        right_key_fn:  Extracts the join key from a right-side tuple.
        combine_fn:    Merges a left tuple and a right tuple into one
                       output tuple. Controls what columns appear in the result.

    Returns:
        RDD of combined tuples produced by combine_fn for every matching pair.
    """

    # ===================== MAP PHASE =====================
    # Tag every row with its join key so Spark knows how to group them.
    # After this step each element is (key, original_row).
    left_keyed = left_rdd.map(lambda t: (left_key_fn(t), t))
    right_keyed = right_rdd.map(lambda t: (right_key_fn(t), t))

    # ===================== SHUFFLE (COGROUP) =====================
    # Spark redistributes data so that all rows sharing the same key
    # end up on the same worker. cogroup returns, for each key, two
    # iterables: one from the left side and one from the right side.
    cogrouped = left_keyed.cogroup(right_keyed)

    # ===================== LOCAL JOIN =====================
    # Within each key group we do a nested-loop (cross product) over
    # the left and right rows. This is fine because only rows with the
    # same key are compared. The expensive cross-partition work was
    # already eliminated by the shuffle above.
    def local_join(pair):
        _, (left_iter, right_iter) = pair
        left_list = list(left_iter)
        right_list = list(right_iter)
        results = []
        for l_tuple in left_list:
            for r_tuple in right_list:
                results.append(combine_fn(l_tuple, r_tuple))
        return results

    return cogrouped.flatMap(local_join)


def run_binary_joins(A, B, C):
    """
    Join three relations — A(x,y), B(y,z), C(z,w) — using two back-to-back
    hash joins at the RDD level.

    Why two joins?  A binary strategy is the simplest way to chain multi-way
    joins: first combine A and B on their shared column y, then take that
    intermediate result and combine it with C on column z.  Each join is a
    full map → shuffle → reduce pass, so this approach costs two shuffles
    in total.

    Args:
        A: DataFrame with columns (x, y).
        B: DataFrame with columns (y, z).
        C: DataFrame with columns (z, w).

    Returns:
        RDD of the final joined result.
    """
    rdd_A = A.rdd
    rdd_B = B.rdd
    rdd_C = C.rdd

    # ===================== JOIN 1: A ⋈ B on y =====================
    # Match rows from A and B that share the same y value.
    # We only keep (x, y, z) as a plain tuple because this is just an
    # intermediate result — no need to build a full Row yet.
    ab_rdd = _hash_join(
        left_rdd=rdd_A,
        left_key_fn=lambda r: r.y,
        right_rdd=rdd_B,
        right_key_fn=lambda r: r.y,
        combine_fn=lambda a, b: (a.x, a.y, b.z),
    )

    # ===================== JOIN 2: AB ⋈ C on z =====================
    # Now join the intermediate (x, y, z) tuples with C on column z.
    # z sits at index 2 in the intermediate tuple, so we pull it out
    # with t[2].  The final combine_fn assembles the full Row(x,y,z,w).
    result_rdd = _hash_join(
        left_rdd=ab_rdd,
        left_key_fn=lambda t: t[2],
        right_rdd=rdd_C,
        right_key_fn=lambda r: r.z,
        combine_fn=lambda ab, c: Row(x=ab[0], y=ab[1], z=ab[2], w=c.w),
    )

    return result_rdd

