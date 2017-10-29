import os
import sys

if len(sys.argv) != 3:
    print 'usage: [host list file] [myria output file]'
me = os.uname()[1]
workers_list = []
counter = 1
with open(sys.argv[1], 'r') as fi:
    for line in fi:
        line = line.strip('\n').strip()
        if len(line) == 0:
            continue
        workers_list.append('%d = %s:9001::myria\n' % (counter, line))
        counter += 1
workers = ''.join(workers_list)
result = '''
# Deployment configuration
[deployment]
path = /tmp/myria
name = twoNodeLocalParallel
dbms = postgresql
database_name = myria
database_password = myria
rest_port = 8753
# Uncomment if need to set a specific username; does not work for localhost
#username = dhalperi

# Compute nodes configuration
[master]
0 = %s:8001

[workers]
%s

[runtime]
# Uncomment to set number of virtual CPU cores used by the master process
container.master.vcores.number = 2
# Uncomment to set number of virtual CPU cores used by the worker processes
container.worker.vcores.number = 2
# Uncomment to set the minimum heap size of the master processes
jvm.master.heap.size.min.gb = 0.9
# Uncomment to set the minimum heap size of the worker processes
jvm.worker.heap.size.min.gb = 0.9
# Uncomment to set the maximum heap size of the master processes
jvm.master.heap.size.max.gb = 3
# Uncomment to set the maximum heap size of the worker processes
jvm.worker.heap.size.max.gb = 3
# Uncomment to set the driver container memory limit
#container.driver.memory.size.gb = 0.5
# Uncomment to set the master container memory limit
container.master.memory.size.gb = 3
# Uncomment to set the worker container memory limit
container.worker.memory.size.gb = 3
# Uncomment to set other JVM options, separate them with space
#jvm.options = -XX:+UseG1GC

[persist]
persist_uri = hdfs://vega.cs.washington.edu:8020
''' % (me, workers)
with open(sys.argv[2], 'w') as fo:
    fo.write(result)
