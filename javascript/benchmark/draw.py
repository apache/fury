import matplotlib.pyplot as plt
import numpy as np
import sys


[_, a1, a2, a3, a4, a5, a6] = sys.argv[0:7]

labels = ['serialize', 'deserialize']
json = [int(a1), int(a2)]
protobuf = [int(a3), int(a4)]
fury = [int(a5), int(a6)]

x = np.arange(len(labels))  # the label locations
width = 0.1  # the width of the bars

fig, ax = plt.subplots(figsize = (7, 7))
rects1 = ax.bar(x - width, json, width, label='json')
rects2 = ax.bar(x, protobuf, width, label='protobuf')
rects3 = ax.bar(x + width, fury, width, label='fury', color="#7845FD")

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Tps/10000')
ax.set_title('javascript complex object', loc="center")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)

ax.legend(loc="upper center")

fig.savefig('./sample.jpg')
