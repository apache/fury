load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("//bazel:fury.bzl", "COPTS")

cc_library(
    name = "libmmh3",
    srcs = ["MurmurHash3.cc"],
    hdrs = ["MurmurHash3.h"],
    strip_include_prefix = "/src",
    copts = COPTS,
    alwayslink=True,
    linkstatic=True,
    visibility = ["//visibility:public"],
)
