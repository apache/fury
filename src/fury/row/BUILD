load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//bazel:fury.bzl", "COPTS")

cc_library(
    name = "fury_row_format",
    srcs = glob(["*.cc"], exclude=["*test.cc"]),
    hdrs = glob(["*.h"]),
    strip_include_prefix = "/src",
    deps = [
      "@local_config_pyarrow//:arrow", "//src/fury/util:fury_util"
    ],
    copts = COPTS,
    visibility = ["//visibility:public"],
)

cc_test(
    name = "row_test",
    srcs = ["row_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_row_format",
        "@com_google_googletest//:gtest",
    ],
)
