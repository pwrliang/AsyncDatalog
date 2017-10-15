package socialite.test;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.async.util.TextUtils;
import socialite.engine.ClientEngine;
import socialite.engine.Config;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;
import socialite.util.AtomicDouble;
import socialite.util.MySTGroupFile;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CC {
    private static final Log L = LogFactory.getLog(SSSP.class);

    //dateset        node
    //livejournal    4847571
    //google         875713
    //berkstan       685230
    public static void main(String[] args) throws FileNotFoundException {
//        test();
        distTest();
    }

    static void test() {
        STGroup stg = new MySTGroupFile(CC.class.getResource("CC.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 4847571;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "hdfs://master:9000/Datasets/CC/LiveJournal/edge.txt");
        st.add("NPATH", "hdfs://master:9000/Datasets/CC/LiveJournal/node.txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);


        LocalEngine en = new LocalEngine(Config.par(64));

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
        System.out.println("cc count:" + result.size());
        en.shutdown();
    }

    static void distTest() {
        STGroup stg = new MySTGroupFile(CC.class.getResource("CC.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 875713;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "hdfs://master:9000/Datasets/CC/Google/edge.txt");
        st.add("NPATH", "hdfs://master:9000/Datasets/CC/Google/node.txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);


        ClientEngine en = new ClientEngine();

        en.run(init);

        st = stg.getInstanceOf("Iter");
        long start = System.currentTimeMillis();
        StopWatch stopWatch = new StopWatch();
        stopWatch.reset();
        stopWatch.start();
        String iterCode = st.render();
        en.run(iterCode);
        stopWatch.stop();
        L.info("recursive statement:" + (System.currentTimeMillis() - start));
//        StringBuilder stringBuilder = new StringBuilder();
//        en.run("?- Comp(n, belong).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                stringBuilder.append(_0.getInt(0)).append(" ").append(_0.getInt(1)).append("\n");
//                return true;
//            }
//        }, 0);
//        TextUtils.writeText("/home/gengl/CC_BERKSTAN", stringBuilder.toString());
        en.shutdown();
    }
}
