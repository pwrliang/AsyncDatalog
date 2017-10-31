package socialite.test;

import org.apache.commons.lang3.time.StopWatch;
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

public class CC {
    private static final Log L = LogFactory.getLog(CC.class);

    //dateset        node
    //livejournal    4847571
    //google         875713
    //berkstan       685230
    //0      1              2             3          4
    //single threadnum      node-count   node-path   edge-path
    //dist node-count      node-path     edge-path
    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
        STGroup stg = new MySTGroupFile(CC.class.getResource("CC.stg"),
                "UTF-8", '<', '>');
        stg.load();
        ST st = stg.getInstanceOf("Init");

        if (args[0].equals("single")) {
            LocalEngine en = new LocalEngine(Config.par(Integer.parseInt(args[1])));
            st.add("N", Integer.valueOf(args[2]));
            st.add("NPATH", args[3]);
            st.add("PATH", args[4]);
            st.add("SPLITTER", "\t");
            String init = st.render();
            L.info(init);
            en.run(init);
            st = stg.getInstanceOf("Iter");
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            String iterCode = st.render();
            en.run(iterCode);
            stopWatch.stop();
            L.info("recursive statement:" + stopWatch.getTime());
            en.shutdown();
        } else if (args[0].equals("dist")){
            st.add("N", Integer.valueOf(args[1]));
            st.add("NPATH", args[2]);
            st.add("PATH", args[3]);
            st.add("SPLITTER", "\t");
            String init = st.render();
            System.out.println(init);
            MasterNode.startMasterNode();
            while (MasterNode.getInstance().getQueryListener().getEngine() == null)//waiting workers online
                Thread.sleep(100);
            ClientEngine en = new ClientEngine();
            en.run(init);
            st = stg.getInstanceOf("Iter");
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            String iterCode = st.render();
            en.run(iterCode);
            stopWatch.stop();
            L.info("recursive statement:" + stopWatch.getTime());
            en.shutdown();
        }
    }

}
