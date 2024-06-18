# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import argparse
import subprocess
import platform
import urllib.request as ulib
import os
import logging
import importlib

BAZEL_VERSION = "6.3.2"

PYARROW_VERSION = "14.0.0"

PROJECT_ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _exec_cmd(cmd: str):
    logging.info(f"running command: {cmd}")
    try:
        result = subprocess.check_output(cmd, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as error:
        logging.error(error.stdout)
        raise

    logging.info(f"command result: {result}")
    return result


def _get_os_name_lower():
    return platform.system().lower()


def _is_windows():
    return _get_os_name_lower() == "windows"


def _get_os_machine():
    machine = platform.machine().lower()
    # Unified to x86_64(Windows return AMD64, others return x86_64).
    return machine.replace("amd64", "x86_64")


def _get_bazel_download_url():
    download_url_base = (
        f"https://github.com/bazelbuild/bazel/releases/download/{BAZEL_VERSION}"
    )
    suffix = "exe" if _is_windows() else "sh"
    return (
        f"{download_url_base}/bazel-{BAZEL_VERSION}{'' if _is_windows() else '-installer'}-"
        f"{_get_os_name_lower()}-{_get_os_machine()}.{suffix}"
    )


def _cd_project_subdir(subdir):
    os.chdir(os.path.join(PROJECT_ROOT_DIR, subdir))


def _run_cpp():
    _install_cpp_deps()
    # run test
    query_result = _exec_cmd("bazel query //...")
    _exec_cmd(
        "bazel test {}".format(query_result.replace("\n", " ").replace("\r", " "))
    )


def _run_rust():
    logging.info("Executing fury rust tests")
    _cd_project_subdir("rust")

    cmds = (
        "cargo doc --no-deps --document-private-items --all-features --open",
        "cargo fmt --all -- --check",
        "cargo fmt --all",
        "cargo clippy --workspace --all-features --all-targets -- -D warnings",
        "cargo doc",
        "cargo build --all-features --all-targets",
        "cargo test",
        "cargo clean",
    )
    for cmd in cmds:
        _exec_cmd(cmd)
    logging.info("Executing fury rust tests succeeds")


def _run_js():
    logging.info("Executing fury javascript tests.")
    _cd_project_subdir("javascript")
    _exec_cmd("npm install")
    _exec_cmd("npm run test")
    logging.info("Executing fury javascript tests succeeds.")


def _install_cpp_deps():
    _exec_cmd(f"pip install pyarrow=={PYARROW_VERSION}")
    _exec_cmd("pip install psutil")
    _exec_cmd("pip install 'numpy<2.0.0'")
    _install_bazel()


def _install_bazel():
    local_name = "bazel.exe" if _is_windows() else "bazel-installer.sh"
    bazel_download_url = _get_bazel_download_url()
    logging.info(bazel_download_url)
    ulib.urlretrieve(bazel_download_url, local_name)
    os.chmod(local_name, 0o777)

    if _is_windows():
        bazel_path = os.path.join(os.getcwd(), local_name)
        _exec_cmd(f'setx path "%PATH%;{bazel_path}"')
    else:
        _exec_cmd(f"./{local_name} --user")
        os.remove(local_name)

    # bazel install status check
    _exec_cmd("bazel --version")

    # default is byte
    psutil = importlib.import_module("psutil")
    total_mem = psutil.virtual_memory().total
    limit_jobs = int(total_mem / 1024 / 1024 / 1024 / 3)
    with open(".bazelrc", "a") as file:
        file.write(f"\nbuild --jobs={limit_jobs}")


def _parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.set_defaults(func=parser.print_help)
    subparsers = parser.add_subparsers()

    cpp_parser = subparsers.add_parser(
        "cpp",
        description="Run C++ CI",
        help="Run C++ CI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    cpp_parser.set_defaults(func=_run_cpp)

    rust_parser = subparsers.add_parser(
        "rust",
        description="Run Rust CI",
        help="Run Rust CI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    rust_parser.set_defaults(func=_run_rust)

    js_parser = subparsers.add_parser(
        "javascript",
        description="Run Javascript CI",
        help="Run Javascript CI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    js_parser.set_defaults(func=_run_js)

    args = parser.parse_args()
    arg_dict = dict(vars(args))
    del arg_dict["func"]
    args.func(**arg_dict)


if __name__ == "__main__":
    _parse_args()
