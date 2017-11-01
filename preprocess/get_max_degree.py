import sys

if len(sys.argv) != 2:
    print 'usage: [edge pair input path]'
    exit(1)
with open(sys.argv[1], 'r') as fi:
    node_dict = {}
    for line in fi:
        tmp = line.split()
        src = int(tmp[0])
        if src not in node_dict:
            node_dict[src] = 1
        else:
            node_dict[src] += 1
    max_degree = 0
    max_node = 0
    for node in node_dict:
        if node_dict[node] > max_node:
            max_degree = node_dict[node]
            max_node = node

    print max_node
    print 'degree ' + str(max_degree)
