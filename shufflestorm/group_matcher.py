from pyspark.rdd import RDD

def create_group_functions(total_rows: int, group_size: int):
    desired_groups = (total_rows + group_size - 1) // group_size
    
    def get_group_id(record_id: int) -> int:
        return (record_id // group_size) + 1

    def group_mapper(record):
        record_id = record[0]
        g_i = get_group_id(record_id)

        emits = []
        for group_id in range(1, desired_groups + 1):
            if g_i != group_id:
                key = tuple(sorted([g_i, group_id]))
                emits.append((key, record))
        return emits  # replication rate = g-1

    def group_reducer(group_data):
        key, records = group_data
        records = list(records)

        # Compare all pairs at this reducer.
        # Intra-group pairs will appear at g-1 reducers each (redundant),
        # but .distinct() in the pipeline removes duplicates.
        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                r1, r2 = records[i], records[j]
                if r1[1] == r2[1]:
                    yield (r1, r2) if r1[0] < r2[0] else (r2, r1)

    return group_mapper, group_reducer


def run_group_matching(data_rdd: RDD, total_rows: int, group_size: int = 100):
    mapper, reducer = create_group_functions(total_rows, group_size)

    return (
        data_rdd
        .flatMap(mapper)          # replication rate r = g-1
        .groupByKey()             # one reducer per pair of groups
        .flatMap(reducer)         # all pairs compared, intra-group redundantly
        .distinct()               # remove duplicate intra-group pairs
    )