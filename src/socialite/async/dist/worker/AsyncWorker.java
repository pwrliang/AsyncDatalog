package socialite.async.dist.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.AsyncAnalysis;
import socialite.parser.Table;
import socialite.resource.SRuntimeWorker;
import socialite.resource.TableInstRegistry;
import socialite.tables.TableInst;

import java.util.Map;

public class AsyncWorker {
    private static final Log L = LogFactory.getLog(AsyncWorker.class);
    private DistAsyncRuntime distAsyncRuntime;

    public AsyncWorker() throws InterruptedException {
        while (SRuntimeWorker.getInst() == null)
            Thread.sleep(100);
        distAsyncRuntime = new DistAsyncRuntime();
    }

    public void startWorker() {
        distAsyncRuntime.run();
    }
}
