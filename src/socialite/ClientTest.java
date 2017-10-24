package socialite;

public class ClientTest {
   static boolean waiting = true;
    public static void main(String[] args) throws InterruptedException {
//        CyclicBarrier barrier = new CyclicBarrier(3,new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("call");
//                Thread thread = new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        System.out.println("in thread");
//                    }
//                });
//                thread.start();
//                try {
//                    Thread.sleep(100);
//                    thread.join();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        IntStream.range(0,3).parallel().forEach(i->{
//            try {
//                barrier.await();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (BrokenBarrierException e) {
//                e.printStackTrace();
//            }
//        });
        final Object obj = new Object();

        new Thread(() -> {
            while (true) {
                synchronized (obj) {
                    while (waiting) {
                        try {
                            obj.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                System.out.println("ok");
                waiting = true;
            }
        }).start();
        while (true) {
            waiting = false;
            synchronized (obj) {
                obj.notify();
            }
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
//        String stats = "seed(int x).\n" +
//                "cite(int y:1..99, int x).\n" +
//                "ancestor(int Y:1..99, int X, int depth).\n" +
//                "cite(y, x) :- l=$read(\"/home/gengl/socialite-before-yarn/examples/prog6_cite.txt\"), (s1,s2)=$split(l, \"\t\"), y=$toInt(s1), x=$toInt(s2).\n" +
//                "seed(x) :- x=5.\n" +
//                "ancestor(Y, X, d) :- cite(Y, X), X<5, d=1.\n";
//        localEngine.run(stats);
//        localEngine.run("?- ancestor(Y, X, d).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        });
//        String stats = "Node(int n:0..875713).\n" +
//                "Rank(int n:0..875713, double rank).\n" +
//                "Edge(int n:0..875713, (int t)).\n" +
//                "EdgeCnt(int n:0..875712, int cnt).\n" +
//                "Edge(s, t) :- l=$read(\"/home/gengl/Datasets/PageRank/Google/sorted.txt\"), (s1,s2)=$split(l, \"\t\"), s=$toInt(s1), t=$toInt(s2).\n" +
//                "Node(n) :- l=$read(\"/home/gengl/Datasets/PageRank/Google/node.txt\"), n=$toInt(l).\n" +
//                "EdgeCnt(s, $inc(1)) :- Edge(s, t).\n" +
//                "Rank(n, r) :- Node(n), r = 0.2 / 875713.\n";
//        localEngine.run(stats);
//        localEngine.run("?- Node(n).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        });
//        localEngine.shutdown();
    }
}
