package socialite.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.dist.master.MasterNode;
import socialite.engine.ClientEngine;
import socialite.engine.Config;
import socialite.engine.LocalEngine;
import socialite.util.MySTGroupFile;

import java.io.FileNotFoundException;

public class PageRank {
    //dateset        node         iter
    //livejournal    4847571      25
    //google         875713       28
    //berkstan       685230       30
    private static final Log L = LogFactory.getLog(PageRank.class);

    // 0       1             2       3           4
    //single threadnum  node-count edge-path   iter-num
    //dist   node-count   edge-path iter-num
    //-Xmx28G -Dsocialite.output.dir=gen -Dsocialite.worker.num=32 -Dsocialite.port=50100 -Dsocialite.master=master -Dlog4j.configuration=file:/home/gengl/socialite-before-yarn/conf/log4j.properties
    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        STGroup stg = new MySTGroupFile(PageRank.class.getResource("PageRank.stg"),
                "UTF-8", '<', '>');
        stg.load();
        if (args[0].equals("single")) {
            LocalEngine en = new LocalEngine(Config.par(Integer.parseInt(args[1])));//config
            int nodeCount = Integer.parseInt(args[2]);

            ST st = stg.getInstanceOf("Init");
            st.add("N", nodeCount);
            st.add("PATH", args[3]);//web-BerkStan_fix
            String init = st.render();
            System.out.println(init);
            en.run(init);

            st = stg.getInstanceOf("Iter");
            st.add("N", nodeCount);
            long start = System.currentTimeMillis();
            for (int i = 0; i < Integer.parseInt(args[4]); i++) {
                st.add("i", i);
                String iterCode = st.render();
                st.remove("i");
                en.run(iterCode);
                System.out.println("iter:" + i);
            }
            System.out.println("recursive statement:" + (System.currentTimeMillis() - start));
//            TIntFloatMap result = new TIntFloatHashMap();
//            double[] vals = new double[1];
//            en.run("?- Rank(n, 0, rank).", new QueryVisitor() {
//
//                @Override
//                public boolean visit(Tuple _0) {
//                    System.out.println(_0);
//                    vals[0] += _0.getDouble(2);
//                    return true;
//                }
//            });
//            System.out.println(vals[0]);
            en.shutdown();
        } else if (args[0].equals("dist")) {
            int nodeCount = Integer.parseInt(args[1]);//875713 berkstan 685230
            ST st = stg.getInstanceOf("Init");
            st.add("N", nodeCount);
            st.add("PATH", args[2]);
            String init = st.render();
            System.out.println(init);
            MasterNode.startMasterNode();
            while (MasterNode.getInstance().getQueryListener().getEngine() == null)//waiting workers online
                Thread.sleep(100);
            ClientEngine en = new ClientEngine();
            en.run(init);
            st = stg.getInstanceOf("Iter");
            st.add("N", nodeCount);
            long start = System.currentTimeMillis();
            for (int i = 0; i < Integer.parseInt(args[3]); i++) {
                st.add("i", i);
                String iterCode = st.render();
                st.remove("i");
                en.run(iterCode);
                System.out.println("iter:" + i);
            }
            L.info("recursive statement:" + (System.currentTimeMillis() - start));

//            TIntFloatMap result = new TIntFloatHashMap();
//            double[] vals = new double[1];
//            en.run("?- Rank(n, 0, rank).", new QueryVisitor() {
//
//                @Override
//                public boolean visit(Tuple _0) {
//                    System.out.println(_0);
//                    vals[0] += _0.getDouble(2);
//                    return true;
//                }
//            }, 0);
//            System.out.println(vals[0]);
            en.shutdown();
        }
    }
}
