import random
import sys

if len(sys.argv) != 4:
    print 'usage: [input missing source edge pair graph] [output fixed edge pair graph] [splitter]'
    exit(1)
splitter = sys.argv[3]
if splitter == '\\s':
    splitter = ' '
elif splitter == '\\t':
    splitter = '\t'
node_set = set()
with open(sys.argv[1], 'r') as fi:
    for line in fi:
        if line.startswith('#') or line.startswith('%'):
            continue
        tmp = line.strip('\n').strip('\r').split(splitter)
        src = int(tmp[0])
        dst = int(tmp[1])
        node_set.add(src)
        node_set.add(dst)
print 'loaded'
reordered_node_dic = {}
count = 0
for node_id in sorted(node_set):
    reordered_node_dic[node_id] = count
    count += 1

print 'tagged'
with open(sys.argv[1], 'r') as fi:
    with open(sys.argv[2], 'w') as fo:
        last = 0
        for line in fi:
            if line.startswith('#') or line.startswith('%'):
                continue
            tmp = line.strip('\n').strip('\r').split(splitter)
            src = int(tmp[0])
            dst = int(tmp[1])
            reordered_src = reordered_node_dic[src]
            reordered_dst = reordered_node_dic[dst]

            if reordered_src - last > 1:
                for fake_node in range(last + 1, reordered_src):
                    fo.write('%d\t%d\n' % (fake_node, random.randrange(0, len(reordered_node_dic))))

            fo.write('%d\t%d\n' % (reordered_src, reordered_dst))
            last = reordered_src

        for fake_p in range(last + 1, len(reordered_node_dic)):
            fo.write('%d\t%d\n' % (fake_p, random.randrange(0, len(reordered_node_dic))))
