import sys

if len(sys.argv) != 3:
    print 'usage: [input edge pair path] [output edge pair path]'
    exit(0)
edge_dict = {}
with open(sys.argv[1], 'r') as fi:
    for line in fi:
        tmp = line.strip('\n').strip('\r').split('\t')
        src = int(tmp[0])
        dst = int(tmp[1])
        if src not in edge_dict:
            tmp = set()
            tmp.add(dst)
            edge_dict[src] = tmp
        else:
            edge_dict[src].add(dst)
        if dst not in edge_dict:
            tmp = set()
            tmp.add(src)
            edge_dict[dst] = tmp
        else:
            edge_dict[dst].add(src)
with open(sys.argv[2], 'w') as fo:
    for src in sorted(edge_dict):
        dst_set = edge_dict[src]
        for dst in dst_set:
            fo.write('%d\t%d\n' % (src, dst))
