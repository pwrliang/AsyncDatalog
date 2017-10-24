import sys

if len(sys.argv) != 4:
    print 'usage: [sssp/pagerank/cc] [node num] [output path]'
    exit(1)

node_num = int(sys.argv[2])
path = sys.argv[3]
if sys.argv[1] == 'sssp':
    with open(path, 'w') as fo:
        for n_id in range(0, node_num):
            if n_id == 0:
                fo.write('%d\t0\n' % n_id)
            else:
                fo.write('%d\t2147483647\n' % n_id)
elif sys.argv[1] == 'pagerank' or sys.argv[1] == 'cc':
    with open(path, 'w') as fo:
        for n_id in range(0, node_num):
            fo.write('%d\n' % n_id)
