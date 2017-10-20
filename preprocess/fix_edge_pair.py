import random
import sys

if len(sys.argv) != 3:
    print 'usage: [input missing source edge pair graph] [output fixed edge pair graph]'
    exit(1)

edge_dic = {}
node_set = set()
with open(sys.argv[1], 'r') as fi:
    for line in fi:
        if line.startswith('#') or line.startswith('%'):
            continue
        tmp = line.strip('\n').strip('\r').split(' ')
        src = int(tmp[0])
        dst = int(tmp[1])
        edge_dic[src] = dst
        node_set.add(src)
        node_set.add(dst)

reordered_node_dic = {}
count = 0
for node_id in sorted(node_set):
    edge_dic[node_id] = count
    count += 1

with open(sys.argv[2], 'w') as fo:
    last = -1
    for src in sorted(edge_dic):
        reordered_src = reordered_node_dic[src]
        reordered_dst = reordered_node_dic[edge_dic[src]]
        if reordered_src - last != 1:
            for fake_node in range(last + 1, reordered_src):
                fo.write('%d\t%d' % (fake_node, random.randrange(0, len(reordered_node_dic))))
        fo.write('%d\t%d\n' % (reordered_src, reordered_dst))
        last = reordered_src

    for fake_p in range(last + 1, len(reordered_node_dic)):
        fo.write('%d\t%d\n' % (fake_p, random.randrange(0, len(reordered_node_dic))))