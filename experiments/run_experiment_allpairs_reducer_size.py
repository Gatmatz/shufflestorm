import subprocess
import re
from pathlib import Path

# All datasets available in the repository, in ascending size order
DATASETS = [
    ("bikedekho-small", 500),
    ("bikedekho",       4786),
    ("bikewale",        9003),
    # ("citeseer",        200000),
]

# Different REDUCER_SIZE values to experiment with
REDUCER_SIZES = [100, 200, 500, 1000]


def update_configurations(dataset: str, dataset_size: int, reducer_size: int):
    config_path = Path("shufflestorm/config/configurations.py")
    with open(config_path, "r") as f:
        content = f.read()

    # Replace the active DATASET assignment (unquoted line, not a comment)
    content = re.sub(
        r"^DATASET\s*=\s*\".*\"",
        f'DATASET = "{dataset}"',
        content,
        flags=re.MULTILINE,
    )

    # Replace the active DATASET_SIZE assignment (non-comment line)
    content = re.sub(
        r"^DATASET_SIZE\s*=\s*\d+",
        f"DATASET_SIZE = {dataset_size}",
        content,
        flags=re.MULTILINE,
    )

    # Replace the REDUCER_SIZE assignment
    content = re.sub(
        r"^REDUCER_SIZE\s*=\s*\d+",
        f"REDUCER_SIZE = {reducer_size}",
        content,
        flags=re.MULTILINE,
    )

    with open(config_path, "w") as f:
        f.write(content)


def main():
    total = len(DATASETS) * len(REDUCER_SIZES)
    run = 0

    for dataset, size in DATASETS:
        for reducer_size in REDUCER_SIZES:
            run += 1
            print(f"\n{'='*65}")
            print(
                f"[{run}/{total}] DATASET = {dataset!r}  (N={size:,})"
                f"  |  REDUCER_SIZE = {reducer_size}"
            )
            print(f"{'='*65}")

            update_configurations(dataset, size, reducer_size)

            try:
                subprocess.run(["uv", "run", "allpairs.py"], check=True)
            except subprocess.CalledProcessError as e:
                print(
                    f"Error running allpairs.py for dataset '{dataset}'"
                    f" with REDUCER_SIZE={reducer_size}: {e}"
                )
                break  # stop iterating reducer sizes for this dataset


if __name__ == "__main__":
    main()
