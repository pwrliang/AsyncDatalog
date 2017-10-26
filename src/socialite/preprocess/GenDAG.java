package socialite.preprocess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class GenDAG {
    public static void main(String[] args) throws IOException {
        int MIN_PER_RANK = 1; /* Nodes/Rank: How 'fat' the DAG should be.  */
        int MAX_PER_RANK = 100;
        int MIN_RANKS = 3;    /* Ranks: How 'tall' the DAG should be.  */
        int MAX_RANKS = 100;
        int PERCENT = 30;     /* Chance of having an Edge.  */
        if (args.length != 2) {
            throw new RuntimeException("usage: [tall] [output path]");
        }

        int i, j, k, nodes = 0;
        Random random = new Random();
//        int ranks = (MIN_RANKS + random.nextInt(MAX_RANKS - MIN_RANKS + 1));
        int ranks = Integer.parseInt(args[0]);
        Set<Integer> node = new HashSet<>();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(args[1]));
        for (i = 0; i < ranks; i++) {
      /* New nodes of 'higher' rank than all nodes generated till now.  */
            int new_nodes = MIN_PER_RANK + random.nextInt(MAX_PER_RANK - MIN_PER_RANK + 1);

      /* Edges from old nodes ('nodes') to new ones ('new_nodes').  */
            for (j = 0; j < nodes; j++)
                for (k = 0; k < new_nodes; k++)
                    if (random.nextInt(100) < PERCENT) {
//                        System.out.printf("  %d -> %d;\n", j, k + nodes); /* An Edge.  */
                        node.add(j);
                        bufferedWriter.write(String.format("%d\t%d\n", j, k + nodes));
                    }
            nodes += new_nodes; /* Accumulate into old node set.  */
        }
        bufferedWriter.close();
        System.out.println("total nodes:" + node.size());
    }
}
