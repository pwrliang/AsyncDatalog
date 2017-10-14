package socialite.test;

import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.async.util.TextUtils;
import socialite.engine.ClientEngine;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;
import socialite.util.MySTGroupFile;

import java.io.FileNotFoundException;

public class PageRank {
    //dateset        node         iter
    //livejournal    4847571      25
    //google         875713       28
    //berkstan       685230       30

    public static void main(String[] args) throws FileNotFoundException {
        distTest();
    }

    //-Xmx28G -Dsocialite.output.dir=gen -Dsocialite.worker.num=32 -Dsocialite.port=50100 -Dsocialite.master=master -Dlog4j.configuration=file:/home/gengl/socialite-before-yarn/conf/log4j.properties
    static void localTest() {
        STGroup stg = new MySTGroupFile(PageRank.class.getResource("PageRank.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 875713;//875713 berkstan 685230
        int iter = 28;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "/home/gengl/Datasets/directed/web-Google_fix.txt");//web-BerkStan_fix
        String init = st.render();
        System.out.println(init);
        LocalEngine en = new LocalEngine();//config

        en.run(init);

        st = stg.getInstanceOf("Iter");
        st.add("N", nodeCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < iter; i++) {
            st.add("i", i);
            String iterCode = st.render();
            st.remove("i");
            en.run(iterCode);
            System.out.println("iter:" + i);
        }
        System.out.println("recursive statement:" + (System.currentTimeMillis() - start));
        TIntFloatMap result = new TIntFloatHashMap();
        double[] vals = new double[1];
        en.run("?- Rank(n, 0, rank).", new QueryVisitor() {

            @Override
            public boolean visit(Tuple _0) {
                vals[0] += _0.getDouble(2);
                return true;
            }
        });
        en.shutdown();
        System.out.println(vals[0]);
    }

    static void distTest() {
        ClientEngine clientEngine = new ClientEngine();
        STGroup stg = new MySTGroupFile(PageRank.class.getResource("PageRank.stg"),
                "UTF-8", '<', '>');
        stg.load();
        int nodeCount = 875713;//875713 berkstan 685230
        int iter = 28;
        ST st = stg.getInstanceOf("Init");
        st.add("N", nodeCount);
        st.add("PATH", "hdfs://master:9000/Datasets/PageRank/Google/edge.txt");//web-BerkStan_fix
        String init = st.render();
        System.out.println(init);

        clientEngine.run(init);

        st = stg.getInstanceOf("Iter");
        st.add("N", nodeCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < iter; i++) {
            st.add("i", i);
            String iterCode = st.render();
            st.remove("i");
            clientEngine.run(iterCode);
            System.out.println("iter:" + i);
        }
        System.out.println("recursive statement:" + (System.currentTimeMillis() - start));
        clientEngine.run("drop Edge.");
        clientEngine.run("drop EdgeCnt.");
        clientEngine.run("drop Node.");
        clientEngine.run("drop Rank.");
        clientEngine.shutdown();
    }
}
