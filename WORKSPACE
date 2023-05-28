workspace(name = "fury")

load("//bazel:fury_deps_setup.bzl", "setup_deps")
setup_deps()

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")
load("//bazel/arrow:pyarrow_configure.bzl", "pyarrow_configure")
bazel_skylib_workspace()
python_configure(name="local_config_python")
pyarrow_configure(name="local_config_pyarrow")
grpc_deps()