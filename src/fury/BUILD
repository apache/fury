load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//bazel:fury.bzl", "COPTS")

cc_library(
    name = "fury",
    copts = COPTS,
    deps = [
      "@local_config_pyarrow//:arrow",
      "//src/fury/row:fury_row_format",
      "//src/fury/vectorized:fury_columar_format",
    ],
    visibility = ["//visibility:public"],
)
