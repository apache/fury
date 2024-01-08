load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "fury",
    deps = [
      "@local_config_pyarrow//:arrow",
      "//src/fury/row:fury_row_format",
      "//src/fury/columnar:fury_columnar_format",
    ],
    visibility = ["//visibility:public"],
)
