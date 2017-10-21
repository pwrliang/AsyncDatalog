import sys

if len(sys.argv) != 4:
    print 'usage: [pagerank/sssp/cc] [src_edge_file] [dst_adj_file]'
    exit(1)

if sys.argv[1] == 'pagerank' or sys.argv[1] == 'cc':
    with open(sys.argv[2], 'r') as fi:
        with open(sys.argv[3], 'w') as fo:
            last = 0
            adj_list = []
            for line in fi:
                tmp = line.strip('\n').split('\t')
                src = int(tmp[0])
                dst = int(tmp[1])

                if src != last:
                    s_adj = ' '.join(map(str, adj_list))
                    fo.write('%d\t%s\n' % (last, s_adj))
                    adj_list = []
                adj_list.append(dst)
                last = src
            fo.write('%d\t%s\n' % (last, ' '.join(map(str, adj_list))))
elif sys.argv[1] == 'sssp':
    with open(sys.argv[2], 'r') as fi:
        with open(sys.argv[3], 'w') as fo:
            last = 0
            adj_weight_list = []
            for line in fi:
                tmp = line.strip('\n').split('\t')
                src = int(tmp[0])
                dst = int(tmp[1])
                weight = int(tmp[2])

                if src != last:
                    fo.write('%d\t%s\n' % (last, ' '.join(map(str, adj_weight_list))))
                    adj_weight_list = []
                adj_weight_list.append(dst)
                adj_weight_list.append(weight)
                last = src
            fo.write('%d\t%s\n' % (last, ' '.join(map(str, adj_weight_list))))
