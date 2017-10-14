package socialite.test;

import org.apache.commons.lang3.time.StopWatch;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.engine.ClientEngine;
import socialite.util.MySTGroupFile;

import java.io.FileNotFoundException;

public class COST {
    public static final int SRC_NODE = 0;

    //dateset        iter
    //1M             31
    //5M             36
    //10M            38
    public static void main(String[] args) throws FileNotFoundException {
        distTest();
    }

    static void test() {

    }

    static void distTest() {
        STGroup stg = new MySTGroupFile(COST.class.getResource("COST.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 50000000;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount + 1);
        st.add("BASIC_PATH", "hdfs://master:9000/Datasets/COST/5000000/basic_" + nodeCount + ".txt");
        st.add("ASSB_PATH", "hdfs://master:9000/Datasets/COST/5000000/assb_" + nodeCount + ".txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);
        ClientEngine en = new ClientEngine();//config
        en.run(init);
        st = stg.getInstanceOf("Iter");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < 49; i++) {
            st.add("i", i);
            String iterCode = st.render();
            st.remove("i");
            en.run(iterCode);
            System.out.println("iter " + i);
        }
        stopWatch.stop();
        System.out.println("elapsed " + stopWatch.getTime());
        en.shutdown();
    }
}
