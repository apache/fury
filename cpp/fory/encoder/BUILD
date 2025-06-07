load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

cc_library(
    name = "fory_encoder",
    srcs = glob(["*.cc"], exclude=["*test.cc"]),
    hdrs = glob(["*.h"]),
    strip_include_prefix = "/cpp",
    deps = [
      "@local_config_pyarrow//:arrow",
      "//cpp/fory/row:fory_row_format",
      "//cpp/fory/meta:fory_meta"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "row_encoder_test",
    srcs = glob(["*_test.cc"]),
    deps = [
        ":fory_encoder",
        "@com_google_googletest//:gtest",
    ],
)
