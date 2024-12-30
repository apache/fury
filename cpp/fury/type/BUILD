load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

cc_library(
    name = "fury_type",
    srcs = glob(["*.cc"], exclude=["*test.cc"]),
    hdrs = glob(["*.h"]),
    copts = ["-mavx2"],  # Enable AVX2 support
    linkopts = ["-mavx2"],  # Ensure linker also knows about AVX2
    strip_include_prefix = "/cpp",
    alwayslink=True,
    linkstatic=True,
    deps = [
    ],
    visibility = ["//visibility:public"],
)
