from pyspark.rdd import RDD

def create_group_functions(total_rows: int, group_size: int):
    """
    Builds mapper and reducer functions for group-based matching.

    Strategy overview:
      - Divide records into g groups of `group_size` each.
      - To guarantee every cross-group pair of records lands on at least
        one reducer, each record is sent to all (g-1) group-pair reducers
        that involve its own group. This gives a replication rate of g-1.
      - Intra-group pairs (two records from the same group) will appear
        at every reducer that contains that group, so they show up g-1
        times. A final distinct() removes those duplicates.

    Args:
        total_rows: Total number of rows in the dataset.
        group_size: Number of records per group.

    Returns:
        A tuple of (mapper_function, reducer_function) to be used in flatMap.
    """
    desired_groups = (total_rows + group_size - 1) // group_size
    
    def get_group_id(record_id: int) -> int:
        """Returns which of the g groups this record belongs to (1-indexed)."""
        return (record_id // group_size) + 1

    def group_mapper(record):
        """
        Sends a record to every reducer that pairs its group with another group.

        For a record in group g_i, we emit it once for every other group,
        keyed by the sorted pair (min, max) of the two group ids. Sorting
        ensures that group pair (A, B) and (B, A) map to the same reducer.
        Total emits per record: g-1 (the replication rate).

        Args:
            record: A single record tuple, e.g., (record_id, ...).

        Returns:
            A list of key-value pairs where key is a tuple of two group IDs
            and value is the original record.
        """
        record_id = record[0]
        g_i = get_group_id(record_id)

        emits = []
        for group_id in range(1, desired_groups + 1):
            if g_i != group_id:
                key = tuple(sorted([g_i, group_id]))
                emits.append((key, record))
        return emits  # replication rate = g-1

    def group_reducer(group_data):
        """
        Compares all records that arrived at this reducer and yields
        exact-name matches.

        Each reducer receives records from exactly two groups. All cross-group
        pairs are compared here correctly. However, intra-group pairs (both
        records from the same group) also end up here — and they appear at
        g-1 different reducers, producing duplicates. Ordering output by
        smaller id first makes those duplicates identical so distinct() can
        remove them downstream.

        Args:
            group_data: A tuple containing (group_pair_key, records_iterable).

        Yields:
            Matched pairs of records, ordered by record ID to ensure
            consistent duplicates for downstream deduplication.
        """
        key, records = group_data
        records = list(records)

        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                r1, r2 = records[i], records[j]
                yield (r1, r2) if r1[0] < r2[0] else (r2, r1)

    return group_mapper, group_reducer


def run_group_matching(data_rdd: RDD, total_rows: int, group_size: int = 100):
    """
    Runs the full group-based matching pipeline on the given RDD.

    This implements a block-nested loop join pattern. It explicitly controls
    how records are replicated (flatMap) and compared locally (flatMap after
    groupByKey) to find matching pairs without a full Cartesian product.

    Args:
        data_rdd:   RDD of records to be matched.
        total_rows: Total number of rows in the RDD, used to calculate group counts.
        group_size: How many records constitute a single group.

    Returns:
        RDD of matched record pairs.
    """
    mapper, reducer = create_group_functions(total_rows, group_size)

    # ===================== MAP PHASE (REPLICATION) =====================
    # Each record is replicated g-1 times, tagged with a group-pair key.
    # This prepares the data so that every cross-group pair ends up on
    # the same partition.
    mapped = data_rdd.flatMap(mapper)

    # ===================== SHUFFLE (GROUP BY KEY) =====================
    # Spark redistributes data so that all records assigned to the same
    # group-pair key end up on the same worker.
    grouped = mapped.groupByKey()

    # ===================== LOCAL MATCHING =====================
    # Within each group-pair, perform a local all-pairs comparison.
    # Cross-group matches are found exactly once. Intra-group matches
    # are found g-1 times (once for every pairing involving their group).
    matched = grouped.flatMap(reducer)

    # ===================== DEDUPLICATION =====================
    # Remove the identical intra-group matches generated across the
    # redundant group-pair comparisons.
    return matched.distinct()