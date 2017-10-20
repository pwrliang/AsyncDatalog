import random
import sys

cites_dic = {}
paper_set = set()
with open(sys.argv[1], 'r') as cites_fi:
    for line in cites_fi:
        if line.startswith("#"):
            continue
        tmp = line.strip('\n').split('\t')
        p1 = int(tmp[0])
        p2 = int(tmp[1])
        cites_dic[p1] = p2
        paper_set.add(p1)
        paper_set.add(p2)

paper_id_dict = {}
counter = 0
for pid in sorted(paper_set):
    paper_id_dict[pid] = counter
    counter += 1
print 'total paper: %d' % len(paper_id_dict)

with open(sys.argv[2], 'w') as fo:
    last = -1
    for p1 in sorted(cites_dic):
        reordered_p1 = paper_id_dict[p1]
        reordered_p2 = paper_id_dict[cites_dic[p1]]
        # ensure every node has out-link
        if reordered_p1 - last != 1:
            if last == -1:
                last = 0
            for fake_p in range(last + 1, reordered_p1):
                fo.write('%d\t%d\n' % (fake_p, random.randrange(0, len(paper_id_dict))))
        fo.write('%d\t%d\n' % (reordered_p1, reordered_p2))
        last = reordered_p1
    if len(paper_id_dict) - last != 1:
        for fake_p in range(last, len(paper_id_dict)):
            fo.write('%d\t%d\n' % (fake_p, random.randrange(0, len(paper_id_dict))))
