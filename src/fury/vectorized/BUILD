load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//bazel:fury.bzl", "COPTS")

cc_library(
    name = "fury_columar_format",
    srcs = ["arrow_writer.cc"],
    hdrs = ["arrow_writer.h"],
    strip_include_prefix = "/src",
    copts = COPTS,
    deps = [
      "@local_config_pyarrow//:arrow", "//src/fury/util:fury_util", "//src/fury/row:fury_row_format"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "arrow_writer_test",
    srcs = [
        "arrow_writer_test.cc",
    ],
    copts = COPTS,
    deps = [
        ":fury_columar_format",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "convert_test",
    srcs = [
        "convert_test.cc",
    ],
    copts = COPTS,
    deps = [
        ":fury_columar_format",
        "@com_google_googletest//:gtest",
    ],
)
