# Shufflestorm

A distributed systems pipeline built on **Apache Spark (PySpark)** covering two assignment parts:

1. **All Pairs** — compares four similarity-matching strategies and benchmarks them on execution time, task count, and shuffle volume.
2. **Triple Join** — computes the ternary join A(x,y) ⋈ B(y,z) ⋈ C(z,w) using three different approaches and compares their performance.

---

## Part 1 — All Pairs

### Matching Strategies

| Strategy | Description |
|---|---|
| **Naïve** | Brute-force pairwise comparison across all records. |
| **Group-based** | Partitions data into fixed-size groups to limit comparisons per reducer. |
| **Spark SQL** | Leverages Spark's Catalyst Optimizer via SQL joins. |
| **Afrati-Ullman** | Multiway join scheme based on the Afrati-Ullman communication model. |

```bash
uv run allpairs.py
```

---

## Part 2 — Triple Join

Computes A(x,y) ⋈ B(y,z) ⋈ C(z,w) where all relations have the same configurable size.

### Join Strategies

| Strategy | Description |
|---|---|
| **Direct Ternary Join** | A single-pass, three-way join without decomposition. |
| **Two Binary Joins** | Decomposes into (A ⋈ B) ⋈ C — two sequential joins. |
| **SQL Query** | Executes the join using a Spark SQL query. |

```bash
uv run triplejoin.py
```

---

## Quick Start

```bash
# Part 1 — All Pairs
uv run allpairs.py

# Part 2 — Triple Join
uv run triplejoin.py
```

Or use the Jupyter notebook for an interactive, step-by-step experience:

```bash
jupyter notebook shufflestorm.ipynb
```

## Configuration

### Part 1

Edit [`shufflestorm/config/configurations.py`](./shufflestorm/config/configurations.py) to change:

- **`DATASET`** / **`DATASET_SIZE`** — choose between `bikewale`, `bikedekho`, `bikedekho-small`, or `citeseer`.
- **`REDUCER_SIZE`** — group size for the group-based matcher.
- **`P_PRIME`** — prime parameter for the Afrati-Ullman mapping.

### Part 2

Edit the `RELATION_SIZE` variable in [`shufflestorm/config/configurations.py`](./shufflestorm/config/configurations.py) to set how many rows each relation (A, B, C) will contain. You can also adjust `NUMBER_OF_REDUCERS`, `B`, and `C` values in the same file.

## Project Structure

```text
├── allpairs.py                  # Entry point — Part 1 (All Pairs)
├── triplejoin.py                # Entry point — Part 2 (Triple Join)
├── shufflestorm.ipynb           # Jupyter notebook for interactive execution & visualization
├── pyproject.toml               # Dependencies (pyspark, numpy, pandas, …)
├── data/                        # CSV datasets (bikewale, bikedekho, citeseer)
├── results/                     # JSON output with benchmark metrics
└── shufflestorm/                # Core package
    ├── config/configurations.py # Dataset & algorithm parameters
    ├── preprocessing/           # CSV cleaning & formatting
    │   └── synthetic_data_generator.py # Generates relations A(x,y), B(y,z), C(z,w)
    ├── matchers/                # Similarity matching implementations
    │   ├── naive_matcher.py     # Naïve pairwise strategy
    │   ├── group_matcher.py     # Group-based strategy
    │   ├── sql_matcher.py       # Spark SQL strategy (All Pairs)
    │   └── afrati_matcher.py    # Afrati-Ullman strategy
    ├── join/                    # Join implementations
    │   ├── ternary_join.py      # Direct ternary join
    │   ├── binary_join.py       # Two consecutive binary joins
    │   └── sql_join.py          # SQL-based ternary join
    └── utils.py                 # Spark session helpers & metrics collection
```

## Requirements

- Python ≥ 3.9
- Java 8 / 11 / 17 (required by Spark)
- [`uv`](https://docs.astral.sh/uv/) (recommended) or `pip`
