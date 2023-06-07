load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("@com_github_grpc_grpc//bazel:cython_library.bzl", "pyx_library")
load("//bazel:fury.bzl", "COPTS")


pyx_library(
    name = "_util",
    srcs = glob([
        "python/pyfury/includes/*.pxd",
        "python/pyfury/_util.pxd",
        "python/pyfury/_util.pyx",
        "python/pyfury/__init__.py",
    ]),
    cc_kwargs = dict(
        copts = COPTS,
        linkstatic = 1,
    ),
    deps = [
        "//src/fury/util:fury_util",
    ],
)

genrule(
    name = "cp_fury_so",
    srcs = [
        ":python/pyfury/_util.so",
    ],
    outs = [
        "cp_fury_py_generated.out",
    ],
    cmd = """
        set -e
        set -x
        WORK_DIR=$$(pwd)
        cp -f $(location python/pyfury/_util.so) "$$WORK_DIR/python/pyfury"
        echo $$(date) > $@
    """,
    local = 1,
    tags = ["no-cache"],
    visibility = ["//visibility:public"],
)
