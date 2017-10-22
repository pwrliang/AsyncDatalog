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
                sDstWeights = tmp[1].strip(' ').split(' ')
                for sDstWeight in sDstWeights:
                    tmp = sDstWeight.split(',')
                    dst = int(tmp[0])
                    weight = int(tmp[1])
                    fo_e.write('%d\t%d\t%d\n' % (src, dst, weight))
