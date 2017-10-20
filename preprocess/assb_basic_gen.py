import random
import sys

if len(sys.argv) != 3:
    print 'usage: [node num] [output folder]'
N = int(sys.argv[1])

start_cost = 1
end_cost = 2
start_num = 1
end_num = 2
basic_cost = []

assb = [(0, 0, 0)]
for i in range(1, N):
    parent = random.randrange(0, i)  # gen parent for node i
    num = random.randrange(start_num, end_num)
    assb.append((i, parent, num))

for i in range(0, N):
    basic_cost.append(random.randrange(start_cost, end_cost))

with open(sys.argv[2] + '/basic_%d.txt' % N, 'w') as f:
    for i in range(0, N):
        f.write('%d\t%d\n' % (i, basic_cost[i]))

with open(sys.argv[2] + '/assb_%d.txt' % N, 'w') as f:
    for entry in assb:
        f.write('%d\t%d\t%d\n' % (entry[0], entry[1], entry[2]))
