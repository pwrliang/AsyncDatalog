package socialite.async.dist.worker;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.Entry;
import socialite.resource.SRuntimeWorker;

public class AsyncWorker {
    private static final Log L = LogFactory.getLog(AsyncWorker.class);
    private DistAsyncRuntime distAsyncRuntime;

    public AsyncWorker() throws InterruptedException {
        while (SRuntimeWorker.getInst() == null)
            Thread.sleep(100);
        L.info("Worker ready");
        distAsyncRuntime = new DistAsyncRuntime();
    }

    public void startWorker() {
        distAsyncRuntime.run();
        L.info("worker " + (MPI.COMM_WORLD.Rank() - 1) + " saving...");
        distAsyncRuntime.getAsyncTable().iterate(Entry.myVisitor);
    }
}
