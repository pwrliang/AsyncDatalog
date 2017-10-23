import random
import sys

if len(sys.argv) != 4:
    print 'usage: [node num] [max node] [output path]'
    exit(1)

node_num = int(sys.argv[1])
max_node = int(sys.argv[2])
node_set = set()
for n in range(0, node_num):
    node_set.add(random.randrange(0, max_node))

with open(sys.argv[3], 'w') as fo:
    for n_id in sorted(node_set):
        fo.write('%d\n' % n_id)
