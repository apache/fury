load("@rules_cc//cc:defs.bzl",  "cc_library", "cc_test")

cc_library(
    name = "fury_columnar_format",
    srcs = ["arrow_writer.cc"],
    hdrs = ["arrow_writer.h"],
    strip_include_prefix = "/cpp",
    deps = [
      "@local_config_pyarrow//:arrow", "//cpp/fury/util:fury_util", "//cpp/fury/row:fury_row_format"
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "arrow_writer_test",
    srcs = [
        "arrow_writer_test.cc",
    ],
    deps = [
        ":fury_columnar_format",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "convert_test",
    srcs = [
        "convert_test.cc",
    ],
    deps = [
        ":fury_columnar_format",
        "@com_google_googletest//:gtest",
    ],
)
