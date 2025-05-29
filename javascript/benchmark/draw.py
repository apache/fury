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
import numpy as np
import sys


[_, a1, a2, a3, a4, a5, a6] = sys.argv[0:7]

labels = ["serialize", "deserialize"]
json = [int(a1), int(a2)]
protobuf = [int(a3), int(a4)]
fory = [int(a5), int(a6)]

x = np.arange(len(labels))  # the label locations
width = 0.1  # the width of the bars

fig, ax = plt.subplots(figsize=(7, 7))
rects1 = ax.bar(x - width, json, width, label="json")
rects2 = ax.bar(x, protobuf, width, label="protobuf")
rects3 = ax.bar(x + width, fory, width, label="fory", color="#7845FD")

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel("Tps/10000")
ax.set_title("javascript complex object", loc="center")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)

ax.legend(loc="upper center")

fig.savefig("./sample.jpg")
