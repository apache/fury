load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("@com_github_grpc_grpc//bazel:cython_library.bzl", "pyx_library")


pyx_library(
    name = "_util",
    srcs = glob([
        "python/pyfury/includes/*.pxd",
        "python/pyfury/_util.pxd",
        "python/pyfury/_util.pyx",
        "python/pyfury/__init__.py",
    ]),
    cc_kwargs = dict(
        linkstatic = 1,
    ),
    deps = [
        "//src/fury/util:fury_util",
    ],
)

pyx_library(
    name = "mmh3",
    srcs = glob([
        "python/pyfury/lib/mmh3/*.pxd",
        "python/pyfury/lib/mmh3/*.pyx",
        "python/pyfury/lib/mmh3/__init__.py",
    ]),
    cc_kwargs = dict(
        linkstatic = 1,
    ),
    deps = [
        "//src/fury/thirdparty:libmmh3",
    ],
)

pyx_library(
    name = "_serialization",
    srcs = glob([
        "python/pyfury/includes/*.pxd",
        "python/pyfury/_util.pxd",
        "python/pyfury/_serialization.pyx",
        "python/pyfury/__init__.py",
    ]),
    cc_kwargs = dict(
        linkstatic = 1,
    ),
    deps = [
        "//src/fury/util:fury_util",
        "@com_google_absl//absl/container:flat_hash_map",
    ],
)

pyx_library(
    name = "_format",
    srcs = glob([
        "python/pyfury/__init__.py",
        "python/pyfury/includes/*.pxd",
        "python/pyfury/_util.pxd",
        "python/pyfury/*.pxi",
        "python/pyfury/format/_format.pyx",
        "python/pyfury/format/__init__.py",
        "python/pyfury/format/*.pxi",
    ]),
    cc_kwargs = dict(
        linkstatic = 1,
    ),
    deps = [
        "//src/fury:fury",
        "@local_config_pyarrow//:python_numpy_headers",
        "@local_config_pyarrow//:arrow_python_shared_library"
    ],
)

genrule(
    name = "cp_fury_so",
    srcs = [
        ":python/pyfury/_util.so",
        ":python/pyfury/lib/mmh3/mmh3.so",
        ":python/pyfury/format/_format.so",
        ":python/pyfury/_serialization.so",
    ],
    outs = [
        "cp_fury_py_generated.out",
    ],
    cmd = """
        set -e
        set -x
        WORK_DIR=$$(pwd)
        cp -f $(location python/pyfury/_util.so) "$$WORK_DIR/python/pyfury"
        cp -f $(location python/pyfury/lib/mmh3/mmh3.so) "$$WORK_DIR/python/pyfury/lib/mmh3"
        cp -f $(location python/pyfury/format/_format.so) "$$WORK_DIR/python/pyfury/format"
        cp -f $(location python/pyfury/_serialization.so) "$$WORK_DIR/python/pyfury"
        echo $$(date) > $@
    """,
    local = 1,
    tags = ["no-cache"],
    visibility = ["//visibility:public"],
)
