package socialite.async.dist.worker;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.MyVisitorImpl;
import socialite.resource.SRuntimeWorker;

public class AsyncWorker {
    private static final Log L = LogFactory.getLog(AsyncWorker.class);
    private DistAsyncRuntime distAsyncRuntime;

    public AsyncWorker() throws InterruptedException {
        while (SRuntimeWorker.getInst() == null)
            Thread.sleep(100);
        L.info("Worker ready");
        distAsyncRuntime = new DistAsyncRuntime();
        L.info("worker " + (MPI.COMM_WORLD.Rank() - 1) + " saving...");
//        distAsyncRuntime.getAsyncTable().iterate(new MyVisitorImpl() {
//
//            @Override
//            public boolean visit(int a1, double a2, double a3) {
//                System.out.println(a1 + " " + a2 + " " + a3);
//                return true;
//            }
//
//            //CC
//            @Override
//            public boolean visit(int a1, int a2, int a3) {
//                System.out.println(a1 + " " + a2 + " " + a3);
//                return true;
//            }
//
//            //COUNT PATH IN DAG
//            @Override
//            public boolean visit(Object a1, int a2, int a3) {
//                System.out.println(a1 + " " + a2 + " " + a3);
//                return true;
//            }
//
//            //PARTY
//            @Override
//            public boolean visit(int a1) {
//                System.out.println(a1);
//                return true;
//            }
//        });

    }

    public void startWorker() {
        distAsyncRuntime.run();
    }
}
