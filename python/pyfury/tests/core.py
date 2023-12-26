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

import pytest

try:
    import pyarrow as pa
except ImportError:
    pa = None


def require_pyarrow(func):
    func = pytest.mark.fury_format(func)
    arrow_installed = False
    if pa is not None and hasattr(pa, "get_library_dirs"):
        arrow_installed = True
    func = pytest.mark.skipif(not arrow_installed, reason="pyarrow not installed")(func)
    return func
