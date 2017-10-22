import sys

if len(sys.argv) != 2:
    print 'usage: [input edge pair path]'
    exit(1)
node_set = set()
with open(sys.argv[1], 'r') as fi:
    for line in fi:
        line = line.strip('\n').strip('\r')
        tmp = line.split('\t')
        src = int(tmp[0])
        node_set.add(src)
print 'node size: %d' % len(node_set)
