package socialite;

import java.io.File;

public class ClientTest {
    public static void main(String[] args) throws InterruptedException {
        File file = new File("/home/gengl/hadoop/share/hadoop/common/lib");
        for (String fileName : file.list()) {
            System.out.println("JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/" + fileName);
        }


        //-Dsocialite.worker.num=8 -Dsocialite.port=50100 -Dsocialite.master=localhost -Dlog4j.configuration=file:-Dsocialite.port=50100 -Dsocialite.master=master -Dlog4j.configuration=file:/home/gengl/socialite-before-yarn/conf/log4j.properties
//        MasterNode.startMasterNode();
//        while (MasterNode.getInstance().getQueryListener().getDistEngine() == null)
//            Thread.sleep(100);
//        ClientEngine clientEngine = new ClientEngine();
////        clientEngine.run(TextUtils.readText(args[0]));
//        clientEngine.run("Middle(int Key, double initD, int adj, int degree).");
//        clientEngine.run("Middle(key, r, adj, degree) :- Rank(key, r), Edge(key, adj), EdgeCnt(key, degree).");
//        clientEngine.run("?- Middle(key, r, adj, degree).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        },0);
//        clientEngine.shutdown();
//        LocalEngine localEngine = new LocalEngine();
//        localEngine.run("Test(int Key:0..875713, boolean come).");
//        localEngine.shutdown();
    }
}
