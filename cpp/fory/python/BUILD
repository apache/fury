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

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")
load("@com_github_grpc_grpc//bazel:cython_library.bzl", "pyx_library")

cc_library(
    name = "_pyfory",
    srcs = ["pyfory.cc"],
    hdrs = ["pyfory.h"],
    alwayslink=True,
    linkstatic=True,
    strip_include_prefix = "/cpp",
    deps = [
        "//cpp/fory/util:fory_util",
        "@local_config_python//:python_headers",
    ],
    visibility = ["//visibility:public"],
)