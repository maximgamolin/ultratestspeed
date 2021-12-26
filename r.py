import json
from itertools import groupby
import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint
from statistics import mean
from collections import defaultdict

file1 = open('result1.json', 'r')
file2 = open('result.json', 'r')

a: dict = json.loads(file1.read())
a.update(json.loads(file2.read()))
print(a.keys())
raw_result = defaultdict(list)
for img, data in a.items():
    for fixture, results in data.items():
        for i in results:
            raw_result[(img, fixture, i['logs']['type'], i['logs']['calc_type'])].append(i['logs']['time'])
db_result = {}
py_result = {}
for k, v in raw_result.items():
    if k[-1] == 'db':
        db_result[(k[0],k[1],k[2])] = mean(v)
    if k[-1] == 'py':
        py_result[(k[0],k[1],k[2])] = mean(v)






labels = ['sync', 'async', 'thread_pool', 'process_pool']

cpython_alpine = [v for k, v in sorted(py_result.items(), key=lambda y: labels.index(y[0][2])) if 'cpython_alpine' in k and 'fourth_fixture.dump' in k]
cpython_ubuntu = [v for k, v in sorted(py_result.items(), key=lambda y: labels.index(y[0][2])) if 'cpython_ubuntu' in k and 'fourth_fixture.dump' in k]
nogil = [v for k, v in sorted(py_result.items(), key=lambda y: labels.index(y[0][2])) if 'nogil' in k and 'fourth_fixture.dump' in k]
pypy = [v for k, v in sorted(py_result.items(), key=lambda y: labels.index(y[0][2])) if 'pypy' in k and 'fourth_fixture.dump' in k]
print(cpython_alpine, cpython_ubuntu, nogil, pypy)
x = np.arange(len(labels))  # the label locations
width = 0.20  # the width of the bars

fig, ax = plt.subplots()
cpython_alpine_rects = ax.bar(x - width - width, cpython_alpine, width, label='cpython_alpine')
cpython_ubuntu_rects = ax.bar(x - width, cpython_ubuntu, width, label='cpython_ubuntu')
nogil_rects = ax.bar(x, nogil, width, label='nogil')
pypy_rects = ax.bar(x + width, pypy, width, label='pypy')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('DB')
ax.set_title('Fourth Fixture 5_000_000 values calculated in py')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(cpython_alpine_rects, padding=3)
ax.bar_label(cpython_ubuntu_rects, padding=3)
ax.bar_label(nogil_rects, padding=3)
ax.bar_label(pypy_rects, padding=3)

fig.tight_layout()

plt.show()
