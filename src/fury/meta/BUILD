load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//bazel:fury.bzl", "COPTS")

cc_library(
    name = "fury_meta",
    srcs = glob(["*.cc"], exclude=["*test.cc"]),
    hdrs = glob(["*.h"]),
    strip_include_prefix = "/src",
    copts = COPTS,
    visibility = ["//visibility:public"],
)

cc_test(
    name = "preprocessor_test",
    srcs = ["preprocessor_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_meta",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "field_info_test",
    srcs = ["field_info_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_meta",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "type_traits_test",
    srcs = ["type_traits_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_meta",
        "@com_google_googletest//:gtest",
    ],
)
