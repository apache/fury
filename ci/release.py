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
import os
import re
import subprocess

PROJECT_ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")


def bump_version(**kwargs):
    new_version = kwargs["version"]
    langs = kwargs["l"]
    if langs == "all":
        langs = ["java", "python", "javascript", "scala", "rust"]
    else:
        langs = langs.split(",")
    for lang in langs:
        if lang == "java":
            bump_java_version(new_version)
        elif lang == "scala":
            _bump_version("scala", "build.sbt", new_version, _bump_scala_version)
        elif lang == "rust":
            _bump_version("rust", "Cargo.toml", new_version, _bump_rust_version)
        elif lang == "python":
            _bump_version(
                "python/pyfury", "__init__.py", new_version, _bump_python_version
            )
        elif lang == "javascript":
            _bump_version(
                "javascript/packages/fury",
                "package.json",
                new_version,
                _bump_js_version,
            )
            _bump_version(
                "javascript/packages/hps", "package.json", new_version, _bump_js_version
            )
        else:
            raise NotImplemented(f"Unsupported {lang}")


def _bump_version(path, file, new_version, func):
    os.chdir(os.path.join(PROJECT_ROOT_DIR, path))
    with open(file, "r") as f:
        lines = f.readlines()
    lines = func(lines, new_version) or lines
    text = "".join(lines)
    with open(file, "w") as f:
        f.write(text)


def bump_java_version(new_version):
    for p in [
        "integration_tests/graalvm_tests",
        "integration_tests/jdk_compatibility_tests",
        "integration_tests/jpms_tests",
        "integration_tests/latest_jdk_tests",
        "integration_tests/latest_jdk_tests",
        "java/benchmark",
    ]:
        _bump_version(p, "pom.xml", new_version, _bump_pom_parent_version)
    os.chdir(os.path.join(PROJECT_ROOT_DIR, "java"))
    subprocess.check_output(
        f"mvn versions:set -DnewVersion={new_version}",
        shell=True,
        universal_newlines=True,
    )


def _bump_pom_parent_version(lines, new_version):
    start_index, end_index = -1, -1
    for i, line in enumerate(lines):
        if "<parent>" in line:
            start_index = i
        if "</parent>" in line:
            end_index = i
            break
    assert start_index != -1
    assert end_index != -1
    for line_number in range(start_index, end_index):
        line = lines[line_number]
        if "version" in line:
            line = re.sub(
                r"(<version>)[^<>]+(</version>)", r"\g<1>" + new_version + r"\2", line
            )
            lines[line_number] = line


def _bump_scala_version(lines, v):
    for index, line in enumerate(lines):
        if "furyVersion = " in line:
            lines[index] = f'val furyVersion = "{v}"\n'
            break
    return lines


def _bump_rust_version(lines, v):
    for index, line in enumerate(lines):
        if "version = " in line:
            lines[index] = f'version = "{v}"\n'
            break
    return lines


def _bump_python_version(lines, v: str):
    for index, line in enumerate(lines):
        if "version = " in line:
            v = v.replace("-alpha", "a")
            v = v.replace("-beta", "b")
            v = v.replace("-rc", "rc")
            lines[index] = f'version = "{v}"\n'
            break


def _bump_js_version(lines, v: str):
    for index, line in enumerate(lines):
        if "version" in line:
            # "version": "0.5.9-beta"
            for x in ["-alpha", "-beta", "-rc"]:
                if x in v and v.split(x)[-1].isdigit():
                    v = v.replace(x, x + ".")
            lines[index] = f'  "version": "{v}",\n'
            break


def _parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.set_defaults(func=parser.print_help)
    subparsers = parser.add_subparsers()
    bump_version_parser = subparsers.add_parser(
        "bump_version",
        description="Bump version",
    )
    bump_version_parser.add_argument("-version", type=str, help="new version")
    bump_version_parser.add_argument("-l", type=str, help="language")
    bump_version_parser.set_defaults(func=bump_version)

    args = parser.parse_args()
    arg_dict = dict(vars(args))
    del arg_dict["func"]
    args.func(**arg_dict)


if __name__ == "__main__":
    _parse_args()
