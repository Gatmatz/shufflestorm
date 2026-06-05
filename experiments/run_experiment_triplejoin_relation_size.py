import subprocess
import re
from pathlib import Path

def update_configurations(relation_size):
    config_path = Path("shufflestorm/config/configurations.py")
    with open(config_path, "r") as f:
        content = f.read()
    
    # Replace the RELATION_SIZE assignment
    updated_content = re.sub(
        r"^RELATION_SIZE\s*=\s*\d+", 
        f"RELATION_SIZE = {relation_size}", 
        content, 
        flags=re.MULTILINE
    )
    
    with open(config_path, "w") as f:
        f.write(updated_content)

def main():
    # A list of sensible relation sizes ranging from small to quite large
    relation_sizes = [10000, 50000, 100000, 200000]
    
    for size in relation_sizes:
        print(f"\n{'='*50}")
        print(f"Running experiment with RELATION_SIZE = {size}")
        print(f"{'='*50}")
        
        # Update the configuration file
        update_configurations(size)
        
        # Run triplejoin.py using uv run
        try:
            subprocess.run(["uv", "run", "triplejoin.py"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running triplejoin.py with {size} relation size: {e}")
            break

if __name__ == "__main__":
    main()
