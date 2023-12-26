load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//bazel:fury.bzl", "COPTS")

cc_library(
    name = "fury_encoder",
    srcs = glob(["*.cc"], exclude=["*test.cc"]),
    hdrs = glob(["*.h"]),
    strip_include_prefix = "/src",
    deps = [
      "@local_config_pyarrow//:arrow", 
      "//src/fury/row:fury_row_format",
      "//src/fury/meta:fury_meta"
    ],
    copts = COPTS,
    visibility = ["//visibility:public"],
)

cc_test(
    name = "row_encoder_test",
    srcs = glob(["*_test.cc"]),
    copts = COPTS,
    deps = [
        ":fury_encoder",
        "@com_google_googletest//:gtest",
    ],
)
