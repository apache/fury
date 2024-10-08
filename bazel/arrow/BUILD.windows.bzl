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

package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_import")

cc_library(
    name = "arrow",
    hdrs = [":arrow_header_include"],
    includes = ["include"],
    deps = [":arrow_shared_library"],
    visibility = ["//visibility:public"],
)

cc_import(
    name = "arrow_shared_library",
    interface_library = ":libarrow_interface",
    shared_library = ":libarrow",
    visibility = ["//visibility:public"],
)

cc_import(
    name = "arrow_python_shared_library",
    interface_library = ":libarrow_python_interface",
    shared_library = ":libarrow_python",
    visibility = ["//visibility:public"],
)

cc_library(
    name = "arrow_header_lib",
    hdrs = [":arrow_header_include"],
    includes = ["include"],
    visibility = ["//visibility:public"],
)

cc_library(
    name="python_numpy_headers",
    hdrs=[":python_numpy_include"],
    includes=["python_numpy_include"],
)

%{ARROW_HEADER_GENRULE}
%{ARROW_LIBRARY_GENRULE}
%{ARROW_ITF_LIBRARY_GENRULE}
%{ARROW_PYTHON_LIBRARY_GENRULE}
%{ARROW_PYTHON_ITF_LIB_GENRULE}
%{PYTHON_NUMPY_INCLUDE_GENRULE}
