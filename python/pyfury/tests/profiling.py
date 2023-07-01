"""
add `# cython: profile=True` to cython file to compile first
"""

import cProfile
import pstats

import pyfury as fury
import pytest
import pyximport
from pyfury.tests.record import create_foo, foo_schema


@pytest.mark.skip(reason="take too long")
def test_encode():
    """add enable cython profile directive to lib.pxi for profiling"""
    encoder = fury.create_row_encoder(foo_schema())
    foo = create_foo()
    iter_nums = 100000
    for _ in range(iter_nums):
        encoder.to_row(foo)


if __name__ == "__main__":
    pyximport.install()
    cProfile.runctx("test_encode()", globals(), locals(), "Profile.prof")

    s = pstats.Stats("Profile.prof")
    s.strip_dirs().sort_stats("time").print_stats()
    s.print_callers()
    test_encode()
