package socialite.async.dist.worker;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.util.TextUtils;
import socialite.resource.SRuntimeWorker;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;

import java.util.StringJoiner;
import java.util.stream.IntStream;

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
        String[] tmp = AsyncConfig.get().getSavePath().split("/");
        StringJoiner stringJoiner = new StringJoiner("/");
        IntStream.range(0, tmp.length - 1).forEach(i -> stringJoiner.add(tmp[i]));
        AsyncConfig asyncConfig = AsyncConfig.get();
        distAsyncRuntime.getAsyncTable().iterateTuple(new QueryVisitor() {
            TextUtils textUtils = new TextUtils(asyncConfig.getSavePath(), "part-" + myWorkerId);
            @Override
            public boolean visit(Tuple _0) {
                if (asyncConfig.isPrintResult())
                    System.out.println(_0.toString());
                textUtils.writeLine(_0.toString());
                return true;
            }

            @Override
            public void finish() {
                textUtils.close();
            }
        });//save result
    }
}
