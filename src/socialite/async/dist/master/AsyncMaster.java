package socialite.async.dist.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.engine.DistAsyncEngine;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;

public class AsyncMaster {
    public static final int ID = 0;
    private static final Log L = LogFactory.getLog(AsyncMaster.class);
    DistAsyncEngine distAsyncEngine;

    public AsyncMaster(String progPath, int workerNum) throws InterruptedException {
        while (MasterNode.getInstance().getQueryListener().getEngine()==null)//waiting workers online
            Thread.sleep(100);
        distAsyncEngine = new DistAsyncEngine(TextUtils.readText(progPath), workerNum);
    }

    public void startMaster() {
        distAsyncEngine.run();
    }

}
