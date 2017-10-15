package socialite.async.dist.worker;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.util.TextUtils;
import socialite.resource.SRuntimeWorker;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;

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
        int myWorkerId = MPI.COMM_WORLD.Rank() - 1;
        distAsyncRuntime.run();
        L.info("worker " + myWorkerId + " saving...");

        AsyncConfig asyncConfig = AsyncConfig.get();

        TextUtils textUtils = null;
        String savePath = asyncConfig.getSavePath();
        if (savePath.length() > 0)
            textUtils = new TextUtils(asyncConfig.getSavePath(), "part-" + myWorkerId);
        if (textUtils != null || asyncConfig.isPrintResult()) {
            TextUtils finalTextUtils = textUtils;
            distAsyncRuntime.getAsyncTable().iterateTuple(new QueryVisitor() {
                @Override
                public boolean visit(Tuple _0) {
                    if (asyncConfig.isPrintResult())
                        System.out.println(_0.toString());
                    if (finalTextUtils != null)
                        finalTextUtils.writeLine(_0.toString());
                    return true;
                }

                @Override
                public void finish() {
                    if (finalTextUtils != null)
                        finalTextUtils.close();
                }
            });//save result
        }
    }
}
