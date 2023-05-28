package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_import")

cc_library(
    name = "arrow",
    hdrs = [":arrow_header_include"],
    includes = ["include"],
    deps = [":arrow_shared_library"],
    visibility = ["//visibility:public"],
)

cc_import(
    name = "arrow_shared_library",
    shared_library = ":libarrow",
    visibility = ["//visibility:public"],
)

cc_import(
    name = "arrow_python_shared_library",
    shared_library = ":libarrow_python",
    visibility = ["//visibility:public"],
)

cc_library(
    name = "arrow_header_lib",
    hdrs = [":arrow_header_include"],
    includes = ["include"],
    visibility = ["//visibility:public"],
)

cc_library(
    name="python_numpy_headers",
    hdrs=[":python_numpy_include"],
    includes=["python_numpy_include"],
)

%{ARROW_HEADER_GENRULE}
%{ARROW_LIBRARY_GENRULE}
%{ARROW_PYTHON_LIBRARY_GENRULE}
%{PYTHON_NUMPY_INCLUDE_GENRULE}
