import sys

if len(sys.argv) != 4 or sys.argv[3] != 'pagerank' and sys.argv[3] != 'sssp':
    print 'usage: [input edge pair file] [output node list file] [pagerank/sssp]'
    exit(1)

node_set = set()
with open(sys.argv[1], 'r') as fi:
    for line in fi:
        tmp = line.strip('\n').strip('\r').split('\t')
        src = int(tmp[0])
        dst = int(tmp[1])
        node_set.add(src)
        node_set.add(dst)

if sys.argv[3] == 'pagerank':
    with open(sys.argv[2], 'w') as fo:
        for node_id in sorted(node_set):
            fo.write('%d\n' % node_id)
elif sys.argv[3] == 'sssp':
    with open(sys.argv[2], 'w') as fo:
        for node_id in sorted(node_set):
            if node_id == 0:
                fo.write('%d\t0\n' % node_id)
            else:
                fo.write('%d\t2147483647\n' % node_id)
