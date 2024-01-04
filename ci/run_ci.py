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
import sys
import psutil
import wget
import os


def _exec_cmd(cmd: list, stdout=subprocess.PIPE):
    ret = subprocess.run(cmd, stdout=stdout)
    print(f'run command: {ret.args}')
    assert ret.returncode == 0
    return ret


def _run_cpp():
    os_name = platform.system().lower()
    if os_name == 'windows':
        bazel_cmd = './bazel.exe'
    else:
        URL = f'https://github.com/bazelbuild/bazel/releases/download/6.3.2/bazel-6.3.2-installer-{os_name}-x86_64.sh'
        wget.download(URL, 'installer.sh')
        os.chmod('installer.sh', 0o700)
        _exec_cmd(['./installer.sh', '--user'])
        bazel_cmd = 'bazel'

    # bazel install check
    bazel_version = _exec_cmd([bazel_cmd, '--version'])
    print(f'bazel version: {str(bazel_version.stdout)}')

    # default is byte
    total_mem = psutil.virtual_memory().total
    limit_jobs = int(total_mem / 1024 / 1024 / 1024 / 3)
    with open('.bazelrc', 'a') as file:
        file.write(f'\nbuild --jobs={limit_jobs}')

    # run test
    query_result = _exec_cmd([bazel_cmd, 'query', '//...'])
    test_cmd = [bazel_cmd, 'test'] + str(query_result.stdout, 'utf-8').splitlines()
    _exec_cmd(test_cmd)


def _run(item):
    if item == 'cpp':
        _run_cpp()


def _parse_args():
    parser = argparse.ArgumentParser()
    choices = ['java', 'cpp']
    parser.add_argument('-i', '--item', choices=choices, help='Specify an item that needs to be run')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()

    if args.item:
        _run(args.item)
    else:
        pass

