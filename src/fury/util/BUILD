
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//bazel:fury.bzl", "COPTS")

cc_library(
    name = "fury_util",
    srcs = ["status.cc", "buffer.cc", "logging.cc", "util.cc"],
    hdrs = ["status.h", "buffer.h", "logging.h", "util.h"],
    strip_include_prefix = "/src",
    copts = COPTS,
    alwayslink=True,
    linkstatic=True,
    deps = [
        "//src/fury/meta:fury_meta",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/debugging:stacktrace",
        "@com_google_absl//absl/debugging:symbolize",
    ],
    visibility = ["//visibility:public"],
)


cc_test(
    name = "util_test",
    srcs = ["util_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "logging_test",
    srcs = ["logging_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "status_test",
    srcs = ["status_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "buffer_test",
    srcs = ["buffer_test.cc"],
    copts = COPTS,
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)
