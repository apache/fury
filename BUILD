load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

COPTS = ["-pthread","-std=c++11", "-D_GLIBCXX_USE_CXX11_ABI=0", "-DNDEBUG"]

cc_library(
    name = "fury_util",
    srcs = glob(["src/fury/util/*.cc"], exclude=["src/fury/util/*test.cc"]),
    hdrs = glob(["src/fury/util/*.h"]),
    strip_include_prefix = "src",
    copts = COPTS,
    alwayslink=True,
    linkstatic=True,
    deps = [
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@com_google_absl//absl/debugging:stacktrace",
        "@com_google_absl//absl/debugging:symbolize",
    ],
    visibility = ["//visibility:public"],
)
