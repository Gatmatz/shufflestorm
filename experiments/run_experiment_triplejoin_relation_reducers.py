import subprocess
import re
import math
from pathlib import Path

def get_b_and_c(num_reducers):
    b = math.isqrt(num_reducers)
    while num_reducers % b != 0:
        b -= 1
    c = num_reducers // b
    return b, c

def update_configurations(num_reducers):
    config_path = Path("shufflestorm/config/configurations.py")
    with open(config_path, "r") as f:
        content = f.read()
    
    # Replace the NUMBER_OF_REDUCERS assignment
    updated_content = re.sub(
        r"^NUMBER_OF_REDUCERS\s*=\s*\d+", 
        f"NUMBER_OF_REDUCERS = {num_reducers}", 
        content, 
        flags=re.MULTILINE
    )
    
    b, c = get_b_and_c(num_reducers)
    
    updated_content = re.sub(
        r"^B\s*=\s*\d+", 
        f"B = {b}", 
        updated_content, 
        flags=re.MULTILINE
    )
    
    updated_content = re.sub(
        r"^C\s*=\s*\d+", 
        f"C = {c}", 
        updated_content, 
        flags=re.MULTILINE
    )
    
    with open(config_path, "w") as f:
        f.write(updated_content)

def main():
    reducers = [4, 12, 24, 36, 100]
    
    for r in reducers:
        print(f"\n{'='*50}")
        print(f"Running experiment with NUMBER_OF_REDUCERS = {r}")
        print(f"{'='*50}")
        
        # Update the configuration file
        update_configurations(r)
        
        # Run triplejoin.py using uv run
        try:
            subprocess.run(["uv", "run", "triplejoin.py"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running triplejoin.py with {r} reducers: {e}")
            break

if __name__ == "__main__":
    main()
