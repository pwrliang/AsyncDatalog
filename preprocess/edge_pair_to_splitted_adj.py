import sys

if len(sys.argv) != 4:
    print 'usage: [input adj file] [output dir] [worker num]'
    exit(1)
WORKER_NUM = int(sys.argv[3])
LINE_NUM = 0
PART_ID = 0
opened_files = [open('%s/part%d' % (sys.argv[2], worker_id), 'w') for worker_id in
                range(0, WORKER_NUM)]
with open(sys.argv[1], 'r') as fi:
    for line in fi:
        src = int(line.split('\t')[0])
        worker_id = src % WORKER_NUM
        opened_files[worker_id].write(line)
