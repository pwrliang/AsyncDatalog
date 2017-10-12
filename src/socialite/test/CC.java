package socialite.test;

import gnu.trove.map.TIntIntMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.lang3.time.StopWatch;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;
import socialite.util.MySTGroupFile;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class CC {
    public static final int SRC_NODE = 0;

    //dateset        node
    //livejournal    4847571
    //google         875713
    //berkstan       685230
    public static void main(String[] args) throws FileNotFoundException {
//        PrintStream printStream = new PrintStream(new FileOutputStream("/home/gengl/out.txt", true));
//        System.setOut(printStream);
        STGroup stg = new MySTGroupFile(CC.class.getResource("CC.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 4847571;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "/home/gengl/Desktop/gengl/Datasets/undirected/LiveJournal-edge1_fix_undirected.txt");
        st.add("NPATH", "/home/gengl/Desktop/gengl/Datasets/undirected/LiveJournal-node.txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);


        LocalEngine en = new LocalEngine();

        en.run(init);

        st = stg.getInstanceOf("Iter");
        long start = System.currentTimeMillis();
        StopWatch stopWatch = new StopWatch();
        stopWatch.reset();
        stopWatch.start();
        String iterCode = st.render();
        en.run(iterCode);
        stopWatch.stop();
        System.out.println("recursive statement:" + (System.currentTimeMillis() - start));
//        en.run("?- Comp(n, id).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                System.out.println(_0.getInt(0) + " " + _0.getInt(1));
//                return true;
//            }
//        });
        en.run("CompIDs(id)      :- Comp(_, id).");
        final int[] count = {0};
        TIntSet result = new TIntHashSet();
        en.run("?- CompIDs(id).", new QueryVisitor() {
            @Override
            public boolean visit(int _0) {
                result.add(_0);
                count[0]++;
                return true;
            }
        });
        System.out.println("cc count:"+result.size());
        en.shutdown();
    }

    static void save(String path, TIntIntMap result) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            int[] keys = result.keys();
            Arrays.sort(keys);
            for (int src : keys) {
                if (src != SRC_NODE && result.get(src) != 0) {
                    writer.write(String.format("%d %d\n", src, result.get(src)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
