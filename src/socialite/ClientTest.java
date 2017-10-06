package socialite;

import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;
import socialite.engine.ClientEngine;
import socialite.engine.LocalEngine;
import socialite.resource.TableInstRegistry;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;

public class ClientTest {
    public static void main(String[] args) throws InterruptedException {
        //-Dsocialite.worker.num=8 -Dsocialite.port=50100 -Dsocialite.master=localhost -Dlog4j.configuration=file:/E:/socialite-dc0d53aa669a1b707d31bdb476c379a2b1ee88be/conf/log4j.properties
//        MasterNode.startMasterNode();
//        while (MasterNode.getInstance().getQueryListener().getDistEngine() == null)
//            Thread.sleep(100);
//        ClientEngine clientEngine = new ClientEngine();
//        clientEngine.run(TextUtils.readText(args[0]));
//        clientEngine.run("Middle(int Key, double initD, int adj, int degree).");
//        clientEngine.run("Middle(key, r, adj, degree) :- Rank(key, r), Edge(key, adj), EdgeCnt(key, degree).");
//        clientEngine.run("?- Middle(key, r, adj, degree).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        },0);
//        clientEngine.shutdown();
        LocalEngine localEngine = new LocalEngine();
        localEngine.run("Test(int Key:0..875713, boolean come).");
        localEngine.shutdown();
    }
}
