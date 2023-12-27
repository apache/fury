# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
