package socialite.test;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.apache.commons.lang3.time.StopWatch;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;
import socialite.util.MySTGroupFile;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class SSSP {
    public static final int SRC_NODE = 0;

    //dateset        node         iter   distSum
    //livejournal    4847571      16     2243012525
    //google         875713       26
    //berkstan       685230       356

    public static void main(String[] args) throws FileNotFoundException {
//        PrintStream printStream = new PrintStream(new FileOutputStream("/home/gengl/out.txt", true));
//        System.setOut(printStream);
        STGroup stg = new MySTGroupFile(SSSP.class.getResource("SSSP.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 4847571;//google 875713 livejournal 4847571 iter 16
        int iter = 16;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "/home/gengl/Desktop/gengl/Datasets/weight/LiveJournal-edge1_fix_weight.txt");
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
//        save("/home/gengl/Desktop/gengl/IdeaProjects/Datasets/sssp-socialite-result", result);
//            System.out.printf("%d %d\n", src, result.get(src));
//        ReadAnswer.load();
//        ReadAnswer.compare(result);
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
