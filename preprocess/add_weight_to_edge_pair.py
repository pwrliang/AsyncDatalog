import random
import sys

if len(sys.argv) != 3:
    print 'usage: [input edge pair file] [output edge pair with weight file]'
    exit(1)

with open(sys.argv[1], 'r') as fi:
    with open(sys.argv[2], 'w') as fo:
        for line in fi:
            tmp = line.strip('\n').strip('\r').split('\t')
            src = int(tmp[0])
            dst = int(tmp[1])
            weight = random.randrange(90, 100)
            fo.write('%d\t%d\t%d\n' % (src, dst, weight))
