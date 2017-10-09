package socialite.async.dist.master;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.dist.worker.AsyncWorker;
import socialite.async.engine.DistAsyncEngine;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;
import socialite.dist.worker.WorkerNode;
import socialite.engine.ClientEngine;
import socialite.engine.Config;
import socialite.util.SociaLiteException;

public class AsyncMaster {
    public static final int ID = 0;
    private static final Log L = LogFactory.getLog(AsyncMaster.class);
    DistAsyncEngine distAsyncEngine;

    public AsyncMaster(String program) throws InterruptedException {
        while (MasterNode.getInstance().getQueryListener().getEngine()==null)//waiting workers online
            Thread.sleep(100);
        L.info("Master ready");
        ClientEngine clientEngine = new ClientEngine();
        clientEngine.run("Test(int i, int j).");
//        distAsyncEngine = new DistAsyncEngine(program);
    }

    public void startMaster() {
//        distAsyncEngine.run();
    }

}
