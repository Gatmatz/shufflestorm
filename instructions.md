# Instructions

## Configuration

Edit [`shufflestorm/config/configurations.py`](./shufflestorm/config/configurations.py) to set the active parameters.

### Part 1 — All Pairs

```python
DATASET = "bikewale"       # Options: "bikedekho-small", "bikedekho", "bikewale", "citeseer"
DATASET_SIZE = 9003        # Must match the row count of the chosen dataset

REDUCER_SIZE = 500         # Group size for the Group-based matcher
P_PRIME = 3                # Prime parameter for the Afrati-Ullman mapper
```

| Dataset | Rows |
|---|---|
| `bikedekho-small` | 500 |
| `bikedekho` | 4 786 |
| `bikewale` | 9 003 |
| `citeseer` | 200 000 |

### Part 2 — Triple Join

```python
RELATION_SIZE = 100000  # Number of rows for each of A, B, C

# Number of reducers for the ternary join
B = 10
C = 10
NUMBER_OF_REDUCERS = 1000
```

`B` and `C` must satisfy `B × C = NUMBER_OF_REDUCERS`.

## Execution

### Part 1 — All Pairs

```bash
uv run allpairs.py
```

Results are saved to `results/<DATASET>_results.json`.

### Part 2 — Triple Join

```bash
uv run triplejoin.py
```

Results are saved to `results/<RELATION_SIZE>_triplejoin_results.json`.

### Batch Experiments

Run automated sweeps over varying parameters using the scripts in `experiments/`:

```bash
# All Pairs — sweep REDUCER_SIZE across all datasets
uv run experiments/run_experiment_allpairs_reducer_size.py

# Triple Join — sweep RELATION_SIZE
uv run experiments/run_experiment_triplejoin_relation_size.py

# Triple Join — sweep NUMBER_OF_REDUCERS (B and C are derived automatically)
uv run experiments/run_experiment_triplejoin_relation_reducers.py
```

### Jupyter Notebook

Open [`shufflestorm.ipynb`](./shufflestorm.ipynb) and run the cells in order:

1. **Clone** — fetches a fresh copy of the repo.
2. **Configure** — override `configurations.py` values from within the notebook.
3. **Install deps** — ensures `pyspark` and friends are available.
4. **Run All Pairs** — executes `allpairs.py`.
5. **Visualize All Pairs** — loads results JSON into a comparison table (pairs found, time, tasks, shuffle bytes).
6. **Configure Triple Join** — set the relation size and reducers.
7. **Run Triple Join** — executes `triplejoin.py`.
8. **Visualize Triple Join** — loads results JSON into a comparison table (rows, time, tasks, shuffle bytes).

## Folder Overview

| Path | Description |
|---|---|
| `allpairs.py` | Entry point — runs all four matching strategies and exports metrics. |
| `triplejoin.py` | Entry point — runs the ternary join benchmark (direct, binary, SQL). |
| `shufflestorm/` | Core package with matcher and join implementations. |
| `shufflestorm/config/` | Runtime parameters (dataset, reducer size, prime, relation size). |
| `shufflestorm/matchers/` | Similarity matching implementations (naïve, group, SQL, Afrati-Ullman). |
| `shufflestorm/join/` | Ternary join implementations (direct, binary, SQL). |
| `shufflestorm/preprocessing/` | CSV cleaning and schema normalization. |
| `shufflestorm/preprocessing/synthetic_data_generator.py` | Generates random relations A(x,y), B(y,z), C(z,w). |
| `shufflestorm/utils.py` | Spark session helpers and metrics collection. |
| `experiments/` | Scripts for automated batch experimentation. |
| `data/` | Input CSV datasets. |
| `results/` | Output JSON benchmark files. |
| `report/` | LaTeX source for the project report. |
