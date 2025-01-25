load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

cc_library(
    name = "fury_util",
    srcs = glob(["*.cc"], exclude=["*test.cc", "benchmark.cc"]),
    hdrs = glob(["*.h"]),
    copts = ["-mavx2"],  # Enable AVX2 support
    linkopts = ["-mavx2"],  # Ensure linker also knows about AVX2
    strip_include_prefix = "/cpp",
    alwayslink=True,
    linkstatic=True,
    deps = [
        "//cpp/fury/meta:fury_meta",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/debugging:stacktrace",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_benchmark//:benchmark",
    ],
    visibility = ["//visibility:public"],
)


cc_test(
    name = "time_util_test",
    srcs = ["time_util_test.cc"],
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "logging_test",
    srcs = ["logging_test.cc"],
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "status_test",
    srcs = ["status_test.cc"],
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "buffer_test",
    srcs = ["buffer_test.cc"],
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "string_util_test",
    srcs = ["string_util_test.cc"],
    deps = [
        ":fury_util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "benchmark",
    srcs = ["benchmark.cc"],
    deps = [
        ":fury_util",
        "@com_google_benchmark//:benchmark",
    ],
)