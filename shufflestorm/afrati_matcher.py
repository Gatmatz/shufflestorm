from pyspark.rdd import RDD
from pyspark.sql import SparkSession

def create_afrati_functions(p: int, d: int):
    """
    Creates and returns the mapper and reducer functions for the Afrati-Ullmann matching algorithm.
    
    - d inputs split into p^2 groups arranged in a p x p grid
    - p+1 teams of p reducers each → p(p+1) reducers total
    - Replication rate: r = p+1
    - Reducer size: q = d/p
    - Requires p to be prime and p^2 to divide d
    """
    assert p * p <= d, "p^2 must divide d"
    group_number = p * p
    group_size = d // group_number  # inputs per group

    def get_group(record_id):
        """
        Determines the grid cell (i, j) for a given record_id.
        Distribute records into p^2 groups, last group absorbs remainder.
        Divide the d inputs into p² equal-sized groups arranged in a square, p on a side. 
        The group in row i and column j is represented by (i, j).
        """
        group_index = min(record_id // group_size, group_number - 1)  # clamp last group
        return group_index // p, group_index % p

    def afrati_mapper(record):
        """Maps a record to multiple reducers based on the Afrati-Ullmann grid allocation strategy."""
        record_id = record[0]
        i, j = get_group(record_id)
        emits = []

        # Teams 0..p-1: send group (i,j) to reducer (k, (i + k*j) mod p)
        for k in range(p):
            reducer_id = (k, (i + k * j) % p)
            emits.append((reducer_id, record))

        # Team p: send group (i,j) to reducer (p, j)
        emits.append(((p, j), record))

        return emits  # exactly p+1 emits per record → replication rate = p+1

    def afrati_reducer(group_data):
        """Reduces group data by yielding pairs of matching records within the same group."""
        key, records = group_data
        records = list(records)

        # Yield only pairs with the same name
        for idx in range(len(records)):
            for jdx in range(idx + 1, len(records)):
                r1, r2 = records[idx], records[jdx]
                if r1[1] == r2[1]:
                    yield (r1, r2) if r1[0] < r2[0] else (r2, r1)

    return afrati_mapper, afrati_reducer


def run_afrati_matching(data_rdd: RDD, p: int, d: int):
    """
    Executes the Afrati-Ullmann matching algorithm on the given RDD.
    p must be prime. Replication rate will be p+1 ≈ d/q + 1,
    matching the lower bound.
    """
    mapper, reducer = create_afrati_functions(p, d)

    return (
        data_rdd
        .flatMap(mapper)
        .groupByKey()
        .flatMap(reducer)
        .distinct()
    )