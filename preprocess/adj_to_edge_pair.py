import sys

if len(sys.argv) != 4:
    print('usage: [adj file path] [edge file path] [pagerank/sssp]')
    exit(1)

if sys.argv[3] == 'pagerank':
    with open(sys.argv[1], 'r') as fi:
        with open(sys.argv[2], 'w') as fo_e:
            for line in fi:
                tmp = line.strip('\n').strip('\r').split('\t')
                src = int(tmp[0])
                sDsts = tmp[1].strip(' ').split(' ')
                for sDst in sDsts:
                    dst = int(sDst)
                    fo_e.write('%d\t%d\n' % (src, dst))
elif sys.argv[3] == 'sssp':
    with open(sys.argv[1], 'r') as fi:
        with open(sys.argv[2], 'w') as fo_e:
            for line in fi:
                tmp = line.strip('\n').strip('\r').split('\t')
                src = int(tmp[0])
                sDsts = tmp[1].strip(' ').split(' ')
                for ind in range(0, len(sDsts) - 1, 2):
                    dst = int(sDsts[ind])
                    weight = int(sDsts[ind + 1])
                    fo_e.write('%d\t%d\t%d\n' % (src, dst, weight))