from myria import MyriaQuery

if __name__ == '__main__':
    MyriaQuery.submit(
        """
        E = load("hdfs://master:9000/Datasets/CC/BerkStan/edge.txt", csv(schema(src:int, dst:int), delimiter="\t"));

        V = [from E emit src as x] + [from E emit dst as x];
        V = select distinct x from V;
        do
          CC = [nid, MIN(cid) as cid] <-
            [from V emit V.x as nid, V.x as cid] +
            [from E, CC where E.src = CC.nid emit E.dst as nid, CC.cid];
        until convergence pull_idb;
        store(CC, CC_output);
                """)
    # hdfs://master:9000/Datasets/CC/BerkStan/edge.txt
