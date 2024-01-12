load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

cc_library(
    name = "fury_encoder",
    srcs = glob(["*.cc"], exclude=["*test.cc"]),
    hdrs = glob(["*.h"]),
    strip_include_prefix = "/cpp",
    deps = [
      "@local_config_pyarrow//:arrow",
      "//cpp/fury/row:fury_row_format",
      "//cpp/fury/meta:fury_meta"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "row_encoder_test",
    srcs = glob(["*_test.cc"]),
    deps = [
        ":fury_encoder",
        "@com_google_googletest//:gtest",
    ],
)
