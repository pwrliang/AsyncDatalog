package socialite.test;

import org.apache.commons.lang3.time.StopWatch;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;
import socialite.util.MySTGroupFile;

import java.io.FileNotFoundException;

public class COST {
    public static final int SRC_NODE = 0;

    //dateset        iter
    //1M             31
    //5M             36
    //10M            38
    public static void main(String[] args) throws FileNotFoundException {
//        PrintStream printStream = new PrintStream(new FileOutputStream("/home/gengl/out.txt", true));
//        System.setOut(printStream);
        STGroup stg = new MySTGroupFile(COST.class.getResource("COST.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 50000000;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount + 1);
        st.add("BASIC_PATH", "/home/gengl/Desktop/gengl/Datasets/prog7/basic_" + nodeCount + ".txt");
        st.add("ASSB_PATH", "/home/gengl/Desktop/gengl/Datasets/prog7/assb_" + nodeCount + ".txt");
        st.add("SPLITTER", "\t");
        String init = st.render();
        System.out.println(init);
        LocalEngine en = new LocalEngine();//config
        en.run(init);
        st = stg.getInstanceOf("Iter");
        StopWatch stopWatch = new StopWatch();
        long[] result = new long[nodeCount];
        stopWatch.start();
        for (int i = 0; i < 49; i++) {
            st.add("i", i);
            String iterCode = st.render();
            st.remove("i");
            en.run(iterCode);
            System.out.println("iter " + i);
            en.run("?- cost(part, " + i + ", cost).", new QueryVisitor() {
                @Override
                public boolean visit(Tuple _0) {
                    result[_0.getInt(0)] += _0.getLong(2);
                    return true;
                }
            });
        }
        stopWatch.stop();
        System.out.println("elapsed " + stopWatch.getTime());
        long max = Integer.MIN_VALUE;
        long min = Integer.MAX_VALUE;
        for (int i = 0; i < result.length; i++) {
            max = Math.max(result[i], max);
            min = Math.min(result[i], min);
            //System.out.println(i + "\t" + result[i]);
        }
        System.out.println("max " + max);
        System.out.println("min " + min);
        en.shutdown();
    }

}
