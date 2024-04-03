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

workspace(name = "fury")

load("//bazel:fury_deps_setup.bzl", "setup_deps")
setup_deps()

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")
load("//bazel/arrow:pyarrow_configure.bzl", "pyarrow_configure")
bazel_skylib_workspace()
python_configure(name="local_config_python")
pyarrow_configure(name="local_config_pyarrow")
grpc_deps()
