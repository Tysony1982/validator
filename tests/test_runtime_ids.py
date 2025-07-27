import math
from src.expectations.validators.column import ColumnMin


def test_runtime_id_uniqueness():
    """Ensure generated runtime IDs are unique.

    A runtime ID uses a 128-bit UUID. The probability of *any* collision in
    ``N`` draws is roughly ``1 - exp(-N*(N-1)/(2*2**128))``. With ``N=10**6``
    this evaluates to about 1.5e-27, well below 1e-18.
    """
    ids = [ColumnMin(column="a", min_value=0).runtime_id for _ in range(1_000_000)]
    assert len(set(ids)) == len(ids)
