import sys

if len(sys.argv) != 3:
    print 'usage: [input edge pair path] [output reversed edge pair path]'
with open(sys.argv[1], 'r') as fi:
    with open(sys.argv[2], 'w') as fo:
        for line in fi:
            line = line.strip('\n').strip('\r')
            tmp = line.split('\t')
            src = int(tmp[0])
            dst = int(tmp[1])
            fo.write('%d\t%d\n' % (dst, src))