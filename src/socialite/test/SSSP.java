package socialite.test;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.apache.commons.lang3.time.StopWatch;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.async.util.TextUtils;
import socialite.engine.ClientEngine;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;
import socialite.util.MySTGroupFile;

import java.io.FileNotFoundException;

public class SSSP {
    public static final int SRC_NODE = 0;

    //dateset        node         iter   distSum
    //livejournal    4847571      16     2243012525
    //google         875713       26     590940903
    //berkstan       685230       356

    public static void main(String[] args) throws FileNotFoundException {
//        PrintStream printStream = new PrintStream(new FileOutputStream("/home/gengl/out.txt", true));
//        System.setOut(printStream);
        test();
//        distTest();
    }

    static void test() {
        STGroup stg = new MySTGroupFile(SSSP.class.getResource("SSSP.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 685230;//google 875713 livejournal 4847571 iter 16
        int iter = 356;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "E:/edge.txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);
        LocalEngine en = new LocalEngine();

        en.run(init, new QueryVisitor() {
            @Override
            public boolean visit(Tuple _0) {
                return super.visit(_0);
            }
        });

        st = stg.getInstanceOf("Iter");
        st.add("N", nodeCount);
        st.add("SRC", SRC_NODE + "");
        long start = System.currentTimeMillis();
        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < iter; i++) {
            stopWatch.reset();
            stopWatch.start();
            st.add("i", i);
            String iterCode = st.render();
            st.remove("i");
            en.run(iterCode);
            stopWatch.stop();
            System.out.println("iter:" + i + " spend " + stopWatch.getTime());
        }
        System.out.println("recursive statement:" + (System.currentTimeMillis() - start));
        TIntIntMap result = new TIntIntHashMap();
        en.run("?- PATH(n, 0, rank).", new QueryVisitor() {
            @Override
            public boolean visit(Tuple _0) {
                result.put(_0.getInt(0), _0.getInt(2));
                return true;

            }
        });
        en.shutdown();

        long distSum = 0;
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        for (int src : result.keys()) {
            if (src != SRC_NODE) {
                max = Math.max(max, result.get(src));
                min = Math.min(min, result.get(src));
//            System.out.println(src + " " + result.get(src));
                distSum += result.get(src);
            }
        }
        System.out.println("max " + max);
        System.out.println("min " + min);
        System.out.println("size " + result.size());
        System.out.println("dist sum " + distSum);
        StringBuilder resultText = new StringBuilder();
        result.forEachEntry((key, val) -> {
            resultText.append(key).append(" ").append(val).append("\n");
            return true;
        });
        TextUtils.writeText("E:\\BerkStanSSSP.txt", resultText.toString());
//        save("/home/gengl/Desktop/gengl/IdeaProjects/Datasets/sssp-socialite-result", result);
//            System.out.printf("%d %d\n", src, result.get(src));
//        ReadAnswer.load();
//        ReadAnswer.compare(result);
    }


    static void distTest() {
        STGroup stg = new MySTGroupFile(SSSP.class.getResource("SSSP.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 4847571;//google 875713 livejournal 4847571 iter 16
        int iter = 16;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "hdfs://master:9000/Datasets/SSSP/Google/edge.txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);
        ClientEngine en = new ClientEngine();

        en.run(init);

        st = stg.getInstanceOf("Iter");
        st.add("N", nodeCount);
        st.add("SRC", SRC_NODE + "");
        long start = System.currentTimeMillis();
        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < iter; i++) {
            stopWatch.reset();
            stopWatch.start();
            st.add("i", i);
            String iterCode = st.render();
            st.remove("i");
            en.run(iterCode);
            stopWatch.stop();
            System.out.println("iter:" + i + " spend " + stopWatch.getTime());
        }
        System.out.println("recursive statement:" + (System.currentTimeMillis() - start));
        en.run("drop Edge.");
        en.run("drop Path.");
        en.shutdown();

    }
}
