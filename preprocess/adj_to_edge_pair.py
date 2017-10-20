import sys

if len(sys.argv) != 3:
    print('usage: [adj file path] [edge file path]')
    exit(1)
with open(sys.argv[1], 'r') as fi:
    with open(sys.argv[2], 'w') as fo_e:
        for line in fi:
            tmp = line.strip('\n').strip('\r').split('\t')
            src = tmp[0]
            sDsts = tmp[1].strip(' ')
            for dst in sDsts:
                fo_e.write('%s\t%s\n' % (src, dst))
