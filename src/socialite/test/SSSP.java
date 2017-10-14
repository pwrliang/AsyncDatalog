package socialite.test;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.engine.ClientEngine;
import socialite.util.MySTGroupFile;

import java.io.FileNotFoundException;

public class SSSP {
    public static final int SRC_NODE = 0;
    private static final Log L = LogFactory.getLog(SSSP.class);
    //dateset        node         iter   distSum
    //livejournal    4847571      16     2243012525
    //google         875713       26     590940903
    //berkstan       685230       356

    public static void main(String[] args) throws FileNotFoundException {
//        PrintStream printStream = new PrintStream(new FileOutputStream("/home/gengl/out.txt", true));
//        System.setOut(printStream);
//        test();
//        distTest();
        test();
    }

    static void test() {
        STGroup stg = new MySTGroupFile(SSSP.class.getResource("SSSP.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 4847571;//google 875713 livejournal 4847571 iter 16
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "hdfs://master:9000/Datasets/SSSP/LiveJournal/edge.txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);
        ClientEngine en = new ClientEngine();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        en.run(init);
        stopWatch.stop();
        L.info("elapsed " + stopWatch.getTime());
        en.shutdown();
//        StringBuilder resultText = new StringBuilder();
//        result.forEachEntry((key, val) -> {
//            resultText.append(key).append(" ").append(val).append("\n");
//            return true;
//        });
//        TextUtils.writeText("E:\\BerkStanSSSP.txt", resultText.toString());
//        save("/home/gengl/Desktop/gengl/IdeaProjects/Datasets/sssp-socialite-result", result);
//            System.out.printf("%d %d\n", src, result.get(src));
//        ReadAnswer.load();
//        ReadAnswer.compare(result);
    }


    static void distTest() {
        STGroup stg = new MySTGroupFile(SSSP.class.getResource("SSSP.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 685230;//google 875713 livejournal 4847571 iter 16
        int iter = 16;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "hdfs://master:9000/Datasets/SSSP/BerkStan/edge.txt");
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
