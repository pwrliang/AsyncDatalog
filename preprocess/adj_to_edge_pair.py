import sys

if len(sys.argv) != 3:
    print('usage: [adj file] [edge file folder]')
    exit(1)
with open(sys.argv[1], 'r') as fi:
    with open(sys.argv[2] + '/edge.txt', 'w') as fo_e:
        with open(sys.argv[2] + '/node.txt', 'w') as fo_n:
            for line in fi:
                tmp = line.strip('\n').split('\t')
                src = tmp[0]
                sDsts = tmp[1].strip(' ')
                for dst in sDsts:
                    fo_e.write('%s\t%s\n' % (src, dst))
                fo_n.write('%s\n' % src)
