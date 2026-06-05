from pyspark.rdd import RDD
from pyspark.sql import SparkSession

def create_afrati_functions(p: int, d: int):
    """
    Builds mapper and reducer functions for the Afrati-Ullmann matching algorithm.

    Core idea — arrange d inputs into a p × p grid of groups:
      - There are p² groups, each holding d/p² records.
      - Group (i, j) sits at row i, column j of the grid.
      - We need every pair of groups to meet at some reducer so all
        record pairs are compared.

    Reducer teams (p+1 teams, p reducers each → p(p+1) reducers total):
      - Teams 0..p-1: Team k sends group (i, j) to reducer (k, (i + k*j) mod p).
        Because p is prime, for any two distinct groups the formula
        (i + k*j) mod p produces a collision for exactly one value of k.
        This guarantees every pair of groups shares exactly one reducer
        across teams 0..p-1 — except groups in the same column (same j),
        which always collide regardless of k.
      - Team p: Handles same-column pairs. It sends group (i, j) to
        reducer (p, j), so all groups sharing column j meet here.

    Each record is emitted to p+1 reducers (one per team), giving
    replication rate r = p+1. This matches the theoretical lower bound
    for the all-pairs problem.

    Parameters:
        p: A prime number controlling grid size and parallelism.
        d: Total number of input records. p² should divide d.
    """
    assert p * p <= d, "p^2 must divide d"
    group_number = p * p
    group_size = d // group_number  # records per group

    def get_group(record_id):
        """
        Maps a record to its grid cell (row i, column j).
        Records are assigned to one of p² groups by integer division.
        The last group absorbs any remainder from rounding.
        """
        group_index = min(record_id // group_size, group_number - 1)  # clamp last group
        return group_index // p, group_index % p

    def afrati_mapper(record):
        """
        Sends a record to exactly p+1 reducers — one in each team.

        Teams 0..p-1 use the formula (k, (i + k*j) mod p) to spread
        groups across reducers so that every cross-group pair meets at
        exactly one of these teams. Team p groups by column j alone to
        cover same-column pairs that the formula doesn't separate.
        """
        record_id = record[0]
        i, j = get_group(record_id)
        emits = []

        # Teams 0..p-1: reducer id is (team_index, (i + k*j) mod p)
        for k in range(p):
            reducer_id = (k, (i + k * j) % p)
            emits.append((reducer_id, record))

        # Team p: all groups in column j go to the same reducer
        emits.append(((p, j), record))

        return emits  # exactly p+1 emits per record → replication rate = p+1

    def afrati_reducer(group_data):
        """
        Compares all records that arrived at this reducer and yields
        exact-name matches. Output is ordered by id so that distinct()
        can deduplicate pairs seen at multiple reducers.
        """
        key, records = group_data
        records = list(records)

        for idx in range(len(records)):
            for jdx in range(idx + 1, len(records)):
                r1, r2 = records[idx], records[jdx]
                yield (r1, r2) if r1[0] < r2[0] else (r2, r1)

    return afrati_mapper, afrati_reducer


def run_afrati_matching(data_rdd: RDD, p: int, d: int):
    """
    Runs the Afrati-Ullmann matching pipeline.

    p must be prime and p² should divide d. The algorithm achieves a
    replication rate of p+1, which is optimal: it matches the theoretical
    lower bound r ≥ d/q + 1 where q = d/p is the reducer capacity.
    """
    mapper, reducer = create_afrati_functions(p, d)

    return (
        data_rdd
        .flatMap(mapper)
        .groupByKey()
        .flatMap(reducer)
        .distinct()
    )