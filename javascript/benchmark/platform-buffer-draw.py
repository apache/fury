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

import matplotlib.pyplot as plt
import sys

[
    browser_utf8_write,
    browser_write,
    native_write,
    browser_to_string,
    native_to_string,
] = sys.argv[1:6]

browser_utf8_write = int(browser_utf8_write)
browser_write = int(browser_write)
native_write = int(native_write)
browser_to_string = int(browser_to_string)
native_to_string = int(native_to_string)

fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(10, 5), sharey=True)

axs[0].bar(
    ["browser utf8Write", "browser write", "native write"],
    [browser_utf8_write, browser_write, native_write],
    color=["b", "g", "r"],
)
axs[0].set_title("Write Comparison")
axs[0].set_xticklabels(["browser utf8Write", "browser write", "native write"])
axs[0].set_ylabel("TPS")


for p in axs[0].patches:
    axs[0].annotate(
        format(p.get_height(), ".0f"),
        (p.get_x() + p.get_width() / 2.0, p.get_height()),
        ha="center",
        va="center",
        xytext=(0, 9),
        textcoords="offset points",
    )

axs[1].bar(
    ["browser toString", "native toString"],
    [browser_to_string, native_to_string],
    color=["b", "r"],
)
axs[1].set_title("toString Comparison")
axs[1].set_xticklabels(["browser toString", "native toString"])

for p in axs[1].patches:
    axs[1].annotate(
        format(p.get_height(), ".0f"),
        (p.get_x() + p.get_width() / 2.0, p.get_height()),
        ha="center",
        va="center",
        xytext=(0, 9),
        textcoords="offset points",
    )

plt.tight_layout()

plt.show()
fig.savefig("./platform-buffer.jpg")
