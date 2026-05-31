# Distributed Systems: Spark-based Data Matching

A comprehensive study project comparing different distributed algorithms for matching records across large datasets using Apache Spark and PySpark. This project implements and benchmarks multiple matching strategies, including naive approaches, group-based MapReduce, Spark SQL optimization, and the Afrati-Ullmann algorithm.

## Overview

The project focuses on the problem of **record matching and deduplication** at scale. It uses motorcycle datasets (BikeDekho and BikeWale) to find duplicate entries across different sources, implementing four distinct algorithmic approaches:

1. **Naive Matcher** - Cartesian product for exhaustive pair matching
2. **Group-Based Matcher** - Optimized MapReduce with partitioning to reduce comparisons
3. **SQL Matcher** - Leveraging Spark SQL with query optimization
4. **Afrati-Ullmann Matcher** - Research-backed bucket-based distribution algorithm

## Project Structure

```
distributed-systems-spark/
├── main.py                          # Entry point for running matchers
├── pyproject.toml                   # Project configuration and dependencies
├── README.md                         # This file
├── data/
│   └── small/
│       ├── bikedekho.csv           # BikeDekho motorcycle dataset
│       └── bikewale.csv            # BikeWale motorcycle dataset
├── distributed_systems_spark/
│   ├── __init__.py
│   ├── utils.py                     # Spark session utilities
│   ├── naive_matcher.py             # Naive Cartesian product approach
│   ├── group_matcher.py             # Group-based MapReduce matcher
│   ├── sql_matcher.py               # Spark SQL matcher with optimizer
│   ├── afrati_matcher.py            # Afrati-Ullmann algorithm implementation
│   ├── config/
│   │   └── settings.py              # Configuration parameters
│   └── preprocessing/
│       └── dataset_preprocessor.py  # Data normalization and preprocessing
└── spark-warehouse/                 # Spark SQL warehouse directory
```

## Setup

### Prerequisites

- Python 3.9+
- Java 8 or later (required for Spark)

### Installation

1. **Install UV** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and navigate to the project**:
   ```bash
   cd distributed-systems-spark
   ```

3. **Create and activate virtual environment**:
   ```bash
   uv venv
   source .venv/bin/activate  # On Linux/Mac
   # or
   .venv\Scripts\activate  # On Windows
   ```

4. **Install dependencies**:
   ```bash
   uv sync
   ```

## Usage

### Running the Application

Execute the main application:
```bash
uv run main.py
```

This will run the currently active matcher (configured in `main.py`). By default, it runs the **Afrati-Ullmann matcher**.

### Configuring Matchers

Edit `main.py` to enable/disable different matchers. Current matchers available:

```python
# Uncomment the desired matcher:

# Naive Matcher (Cartesian product)
# naive_res = run_naive_matching(spark=spark, data_rdd=data_rdd)

# Group-Based Matcher (MapReduce partitioning)
# group_res = run_group_matching(spark, data_rdd=data_rdd, total_rows=DATASET_SIZE, desired_groups=N_GROUPS)

# SQL Matcher (Spark SQL optimization)
# sql_res = run_sql_matching(spark=spark, dataframe=dataframe, explain=False)

# Afrati-Ullmann Matcher (Active by default)
group_res = run_afrati_matching(spark, data_rdd=data_rdd, num_buckets=5)
```

### Configuration Parameters

Edit `distributed_systems_spark/config/settings.py` to customize:

```python
DATASET = "bikewale"              # Dataset name (without .csv extension)
DATASET_SIZE = 9003               # Total rows in dataset
N_GROUPS = 30                      # Number of groups for group-based matching
Q_REDUCER_CAPACITY = 500           # Reducer capacity for Afrati-Ullmann
NUM_GROUPS = int(DATASET_SIZE / Q_REDUCER_CAPACITY)  # Auto-calculated groups
```

## Algorithms

### 1. Naive Matcher
- **Approach**: Cartesian product (cross join) of the entire dataset with itself
- **Time Complexity**: O(n²)
- **Space Complexity**: O(n²)
- **Best for**: Understanding baseline performance; small datasets
- **Drawback**: Generates all possible pairs; massive communication overhead

```python
naive_pairs_rdd = data_rdd.cartesian(data_rdd)
```

### 2. Group-Based Matcher
- **Approach**: Partitions data into groups; applies MapReduce to reduce comparisons
- **Time Complexity**: O(n²/g) where g is number of groups
- **Space Complexity**: O(n²/g)
- **Best for**: Medium-sized datasets; balanced performance
- **Strategy**: 
  - Maps records to group pairs
  - Performs intra-group and inter-group comparisons
  - Avoids duplicate pair generation

### 3. SQL Matcher
- **Approach**: Uses Spark SQL with Catalyst optimizer
- **Time Complexity**: Depends on query plan; typically O(n²) worst case
- **Best for**: Exploiting Spark's query optimizer; cleaner syntax
- **Features**:
  - Physical execution plan visualization
  - Automatic optimization by Catalyst
  - Straightforward SQL semantics

```python
CROSS JOIN with b1.id < b2.id
```

### 4. Afrati-Ullmann Algorithm
- **Approach**: Bucket-based distribution with grid-structured reducers
- **Time Complexity**: O(n² / num_buckets)
- **Space Complexity**: O(n / num_buckets) per reducer
- **Best for**: Large-scale distributed systems; theoretical efficiency bounds
- **Features**:
  - Deterministic hash-based bucketing
  - Grid-arranged reducer pairs (i, j where i ≤ j)
  - Handles same-bucket and cross-bucket pairs efficiently
  - Research-backed from: Afrati et al. "Massively Parallel Similarity Join"

## Data Preprocessing

The project includes a preprocessing script to normalize datasets:

```bash
python -m distributed_systems_spark.preprocessing.dataset_preprocessor
```

This script:
- Keeps only `id` and `bike_name` columns
- Renames `bike_name` to `name`
- Re-indexes records from 0 to N incrementally
- Processes both BikeDekho and BikeWale CSV files

## Output

Each matcher returns an RDD or DataFrame of matched pairs containing:
- `rdd1_id`: ID from first dataset
- `bike1_name`: Bike name from first dataset
- `rdd2_id`: ID from second dataset
- `bike2_name`: Bike name from second dataset

Example output:
```
Afrati-Ullmann Approach Finished. Pairs Found: 1234 | Time: 2.3456s
```

## Performance Comparison

When running multiple matchers, compare their performance metrics:

| Matcher | Time (s) | Pairs Found | Scalability |
|---------|----------|-------------|-------------|
| Naive | Slow | All pairs | O(n²) |
| Group-Based | Medium | Filtered pairs | O(n²/g) |
| SQL | Medium-Fast | Query-dependent | Optimizer-dependent |
| Afrati-Ullmann | Fast | Distributed | O(n²/b) |

## Dependencies

- **pyspark** >= 3.5.0 - Apache Spark Python API
- **pandas** >= 2.3.3 - Data manipulation and CSV handling
- **numpy** >= 2.0.2 - Numerical computing
- **python-dotenv** >= 1.0.0 - Environment variable management

See `pyproject.toml` for the complete dependency specification.

## Adding Dependencies

To add a new dependency:
```bash
uv pip install package-name
```

To add a development dependency:
```bash
uv pip install --group dev package-name
```

## Development

### Code Structure

- **Matchers**: Each algorithm is implemented in its own module (`*_matcher.py`)
- **Utils**: Shared utilities like `get_spark_session()` in `utils.py`
- **Config**: Centralized configuration in `config/settings.py`
- **Preprocessing**: Data pipeline in `preprocessing/`

### Extending the Project

To add a new matching algorithm:

1. Create `new_matcher.py` in `distributed_systems_spark/`
2. Implement the matching function with signature:
   ```python
   def run_new_matching(spark: SparkSession, data_rdd: RDD, **kwargs) -> RDD:
       # Implementation
       return result_rdd
   ```
3. Import and call in `main.py`
4. Compare performance against existing matchers

## References

- **Afrati et al.** - "Massively Parallel Similarity Join" (Research foundation for Afrati-Ullmann)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [MapReduce Algorithms](https://en.wikipedia.org/wiki/MapReduce)

## Notes

- The project uses local Spark mode (`local[*]`) for development. For production, configure the master URL appropriately (YARN, Kubernetes, etc.)
- Spark SQL Adaptive Query Execution is enabled for better optimization
- All matchers preserve pair ordering where `id1 < id2` to avoid duplicate matches

## License

This is an educational project for studying distributed systems and Spark algorithms.

## Author

George - Distributed Systems Study Project

