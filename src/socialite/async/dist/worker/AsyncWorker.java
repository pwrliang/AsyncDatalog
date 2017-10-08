package socialite.async.dist.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.resource.SRuntimeWorker;

public class AsyncWorker {
    private static final Log L = LogFactory.getLog(AsyncWorker.class);
    public final int workerId;
    private DistAsyncRuntime distAsyncRuntime;

    public AsyncWorker(int workerId, int workerNum, int threadNum) throws InterruptedException {
        this.workerId = workerId;
        while (SRuntimeWorker.getInst() == null)
            Thread.sleep(100);
        distAsyncRuntime = new DistAsyncRuntime(SRuntimeWorker.getInst() .getSliceMap(),);

    }

    public void startWorker() {
        //local byte code
        distAsyncRuntime.run();
    }
}
