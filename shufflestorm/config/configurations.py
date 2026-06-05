# ========== Part 1: All Pairs ==========
# Dataset Parameters

# DATASET = "bikedekho-small"
# DATASET_SIZE = 500

# DATASET = "bikedekho"
# DATASET_SIZE = 4786

# DATASET = "bikewale"
# DATASET_SIZE = 9003

DATASET = "bikewale"
DATASET_SIZE = 9003

# For Group-Based Matcher
REDUCER_SIZE = 500

# For Afrati-Ullman Model
P_PRIME = 3 # might change to 5

# ========== Part 2: Triple Join ==========
RELATION_SIZE = 100000  # Number of rows for each of A, B, C

# Number of reducers for the ternary join
B = 10
C = 10
NUMBER_OF_REDUCERS = 1000