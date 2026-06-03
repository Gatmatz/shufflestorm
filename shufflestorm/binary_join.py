"""
Two consecutive binary joins: (A(x, y) ⋈ B(y, z)) ⋈ C(z, w)

Decomposes the three-way join into two sequential binary joins:
  Step 1: AB = A ⋈ B  (join on y)
  Step 2: result = AB ⋈ C  (join on z)
"""


def run_binary_joins(A, B, C):
    """
    Execute A ⋈ B ⋈ C as two consecutive binary joins.

    Args:
        A: Spark DataFrame with columns (x, y).
        B: Spark DataFrame with columns (y, z).
        C: Spark DataFrame with columns (z, w).

    Returns:
        Spark DataFrame containing the join result with columns (x, y, z, w).
    """
    # TODO: Step 1 — join A with B on column y
    # TODO: Step 2 — join the intermediate result with C on column z
    raise NotImplementedError("run_binary_joins is not yet implemented")
