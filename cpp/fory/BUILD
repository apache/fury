load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "fory",
    deps = [
      "@local_config_pyarrow//:arrow",
      "//cpp/fory/row:fory_row_format",
      "//cpp/fory/columnar:fory_columnar_format",
    ],
    visibility = ["//visibility:public"],
)
