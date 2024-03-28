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

import glob
import io
import os
import platform
import re
import shutil
import sys
from os.path import abspath
from os.path import join as pjoin

import setuptools
from setuptools import find_packages  # Must import before Cython
from distutils.core import setup
import Cython
import numpy as np
import pyarrow as pa
from Cython.Build import cythonize

try:
    pa.create_library_symlinks()  # for pyarrow 1.0
except FileExistsError:
    pass

pyarrow_version = "12.0.0"
# Check if we're running 64-bit Python
if not sys.maxsize > 2**32:
    raise RuntimeError("Not supported on 32-bit")

if Cython.__version__ < "0.29":
    raise Exception("Please upgrade to Cython 0.29 or newer")

DEBUG = os.environ.get("FURY_DEBUG", "False").lower() == "true"
BAZEL_BUILD_EXT = os.environ.get("BAZEL_BUILD_EXT", "True").lower() == "true"
if DEBUG:
    os.environ["CFLAGS"] = "-O0"
    BAZEL_BUILD_EXT = False
print("DEBUG = {}, BAZEL_BUILD_EXT = {}".format(DEBUG, BAZEL_BUILD_EXT))

setup_dir = abspath(os.path.dirname(__file__))
print("setup_dir", setup_dir)
project_dir = abspath(pjoin(setup_dir, os.pardir))
fury_cpp_src_dir = abspath(pjoin(setup_dir, "../src/"))
print("fury_cpp_src_dir", fury_cpp_src_dir)

# Try to clean up build directory
shutil.rmtree(pjoin(setup_dir, "build"), ignore_errors=True)
shutil.rmtree(pjoin(setup_dir, "pyfury.egg-info"), ignore_errors=True)

ext_modules = []
if BAZEL_BUILD_EXT:
    import subprocess

    subprocess.check_call(["bazel", "build", "-s", "//:cp_fury_so"])
else:
    ext_modules = cythonize(
        ["pyfury/_util.pyx", "pyfury/_format.pyx", "pyfury/_serialization.pyx"],
        gdb_debug=DEBUG,
    )
    for ext in ext_modules:
        # The Numpy C headers are currently required
        ext.include_dirs.append(np.get_include())
        ext.include_dirs.append(pa.get_include())
        ext.include_dirs.append(fury_cpp_src_dir)
        ext.libraries.extend(pa.get_libraries())
        ext.library_dirs.extend(pa.get_library_dirs())
        ext.sources.extend(
            set(glob.glob(fury_cpp_src_dir + "/**/*.cc", recursive=True))
            - set(glob.glob(fury_cpp_src_dir + "/**/*test.cc", recursive=True))
        )
        print("ext.sources", ext.sources)
        if platform.system() == "Darwin":
            ext.extra_compile_args.append("-stdlib=libc++")
        if os.name == "posix":
            ext.extra_compile_args.append("-std=c++17")
        print("ext.extra_compile_args", ext.extra_compile_args)

        # Avoid weird linker errors or runtime crashes on linux
        ext.define_macros.append(("_GLIBCXX_USE_CXX11_ABI", "0"))


class BinaryDistribution(setuptools.Distribution):
    def has_ext_modules(self):
        return True


def parse_version():
    __init__file = os.path.join(os.path.dirname(__file__), "pyfury/__init__.py")
    with open(__init__file) as f:
        code = f.read()
        match = re.search(r'__version__ = "(.*)"', code)
        return match.group(1)


_pkg_files = [
    "**/*.pxd",
    "**/*.pyx",
    "**/*.pxd",
    "*.so",
    "*.dylib",
    "*.dll",
]


long_description = io.open(
    os.path.join(setup_dir, os.path.pardir, "README.md"), "r", encoding="utf-8"
).read()
disclaimer = io.open(
    os.path.join(setup_dir, os.path.pardir, "DISCLAIMER"), "r", encoding="utf-8"
).read()
long_description += "\n" + disclaimer


setup(
    name="pyfury",
    version=parse_version(),
    author="chaokunyang",
    author_email="shawn.ck.yang@gmail.com",
    maintainer="https://github.com/chaokunyang",
    maintainer_email="shawn.ck.yang@gmail.com",
    package_data={
        "pyfury": _pkg_files,
        "pyfury.format": _pkg_files,
        "pyfury.lib.mmh3": _pkg_files,
    },
    include_package_data=True,
    packages=find_packages(),
    description="Apache Fury™(incubating) is a blazingly fast multi-language serialization "
    + "framework powered by jit and zero-copy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="fury serialization multi-language arrow row-format jit "
    + "codegen polymorphic zero-copy",
    classfiers=[
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    zip_safe=False,
    install_requires=[
        'dataclasses; python_version<"3.7"',
        'pickle5; python_version<"3.8"',
        "cloudpickle",
    ],
    extras_require={
        "format": [f"pyarrow == {pyarrow_version}"],
        "all": [f"pyarrow == {pyarrow_version}"],
    },
    setup_requires=[
        "Cython",
        "wheel",
        f"pyarrow == {pyarrow_version}",
        "numpy" 'dataclasses; python_version<"3.7"',
    ],
    distclass=BinaryDistribution,
    ext_modules=ext_modules,
    license="https://www.apache.org/licenses/LICENSE-2.0",
)

if os.path.exists(pjoin(setup_dir, "pyfury", "_fury.cpp")):
    os.remove(pjoin(setup_dir, "pyfury", "_fury.cpp"))
