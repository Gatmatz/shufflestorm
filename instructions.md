# Instructions

## Configuration

### Part 1 — All Pairs

Edit [`shufflestorm/config/settings.py`](./shufflestorm/config/settings.py) to set the active dataset and algorithm parameters:

```python
DATASET = "bikewale"       # Options: "bikedekho-small", "bikedekho", "bikewale", "citeseer"
DATASET_SIZE = 9003        # Must match the row count of the chosen dataset

REDUCER_SIZE = 200         # Group size for the Group-based matcher
P_PRIME = 3                # Prime parameter for the Afrati-Ullman mapper
```

| Dataset | Rows |
|---|---|
| `bikedekho-small` | 500 |
| `bikedekho` | 4 786 |
| `bikewale` | 9 003 |
| `citeseer` | 200 000 |

### Part 2 — Triple Join

Open [`triplejoin.py`](./triplejoin.py) and set the relation size at the top of the file:

```python
RELATION_SIZE = 1000  # Number of rows for each of A, B, C
```

## Execution

### Part 1 — All Pairs (CLI)

```bash
uv run allpairs.py
```

Results are saved to `results/<DATASET>_results.json`.

### Part 2 — Triple Join (CLI)

```bash
uv run triplejoin.py
```

Results are saved to `results/<RELATION_SIZE>_triplejoin_results.json`.

### Jupyter Notebook

Open [`shufflestorm_runner.ipynb`](./shufflestorm_runner.ipynb) and run the cells in order:

1. **Clone** — fetches a fresh copy of the repo.
2. **Configure** — override `settings.py` values from within the notebook.
3. **Install deps** — ensures `pyspark` and friends are available.
4. **Run All Pairs** — executes `allpairs.py`.
5. **Visualize All Pairs** — loads results JSON into a comparison table (pairs found, time, tasks, shuffle bytes).
6. **Configure Triple Join** — set the relation size.
7. **Run Triple Join** — executes `triplejoin.py`.
8. **Visualize Triple Join** — loads results JSON into a comparison table (rows, time, tasks, shuffle bytes).

## Folder Overview

| Path | Description |
|---|---|
| `allpairs.py` | Entry point — runs all four matching strategies and exports metrics. |
| `triplejoin.py` | Entry point — runs the ternary join benchmark (direct, binary, SQL). |
| `shufflestorm/` | Core package with matcher and join implementations. |
| `shufflestorm/config/` | Runtime parameters (dataset, reducer size, prime). |
| `shufflestorm/preprocessing/` | CSV cleaning and schema normalization. |
| `shufflestorm/data_generator.py` | Generates random relations A(x,y), B(y,z), C(z,w). |
| `shufflestorm/ternary_join.py` | Direct three-way join implementation. |
| `shufflestorm/binary_join.py` | Two consecutive binary joins implementation. |
| `shufflestorm/sql_join.py` | SQL-based join using Spark SQL. |
| `data/` | Input CSV datasets. |
| `results/` | Output JSON benchmark files. |
