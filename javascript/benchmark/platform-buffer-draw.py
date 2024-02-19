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

[_, browser_utf8_write, browser_write, browser_write_1, native_write, browser_to_string, native_to_string] = sys.argv[0:7]

# 创建图形和子图
fig, axs = plt.subplots(nrows=1, ncols=3, figsize=(15, 5), sharey=True)

# 绘制第一部分比较：browser utf8Write 和 browser write
axs[0].bar(['browser utf8Write', 'browser write'], [browser_utf8_write, browser_write], color=['b', 'g'])
axs[0].set_title('Browser UTF8 Write vs Browser Write')
axs[0].set_xlabel('Operation Type')
axs[0].set_xticklabels(['browser utf8Write', 'browser write'])
axs[0].set_ylabel("Tps")


# 在柱形图上添加数值标签
for p in axs[0].patches:
    axs[0].annotate(format(p.get_height(), '.0f'),
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha = 'center', va = 'center',
                    xytext = (0, 9),
                    textcoords = 'offset points')

# 绘制第二部分比较：browser write 和 native write
axs[1].bar(['browser write', 'native write'], [browser_write_1, native_write], color=['g', 'r'])
axs[1].set_title('Browser Write vs Native Write')
axs[1].set_xlabel('Operation Type')
axs[1].set_xticklabels(['browser write', 'native write'])

# 在柱形图上添加数值标签
for p in axs[1].patches:
    axs[1].annotate(format(p.get_height(), '.0f'),
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha = 'center', va = 'center',
                    xytext = (0, 9),
                    textcoords = 'offset points')

# 绘制第三部分比较：browser toString 和 native toString
axs[2].bar(['browser toString', 'native toString'], [browser_to_string, native_to_string], color=['b', 'r'])
axs[2].set_title('Browser ToString vs Native ToString')
axs[2].set_xlabel('Operation Type')
axs[2].set_xticklabels(['browser toString', 'native toString'])

# 在柱形图上添加数值标签
for p in axs[2].patches:
    axs[2].annotate(format(p.get_height(), '.0f'),
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha = 'center', va = 'center',
                    xytext = (0, 9),
                    textcoords = 'offset points')

# 调整布局以避免重叠
plt.tight_layout()

# 显示图形
plt.show()
fig.savefig("./platform-buffer.jpg")
