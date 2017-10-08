package socialite.async.dist.master;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.dist.worker.AsyncWorker;
import socialite.async.engine.DistAsyncEngine;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;
import socialite.dist.worker.WorkerNode;
import socialite.engine.Config;
import socialite.util.SociaLiteException;

public class AsyncMaster {
    public static final int ID = 0;
    private static final Log L = LogFactory.getLog(AsyncMaster.class);
    DistAsyncEngine distAsyncEngine;

    public AsyncMaster(String program) throws InterruptedException {
        while (MasterNode.getInstance().getQueryListener().getEngine()==null)//waiting workers online
            Thread.sleep(100);
        distAsyncEngine = new DistAsyncEngine(program);
    }

    public void startMaster(String[] args) {
        MPI.Init(args);
        int machineNum = MPI.COMM_WORLD.Size();
        int machineId = MPI.COMM_WORLD.Rank();
        int workerNum = machineNum - 1;
        System.setProperty("socialite.worker.num", workerNum + "");
        L.info("Machine "+machineId+" Xmx "+Runtime.getRuntime().maxMemory()/1024/1024);
        if (machineNum - 1 != Config.getWorkerNodeNum())
            throw new SociaLiteException(String.format("MPI Workers (%d)!= Socialite Workers (%d)", workerNum, Config.getWorkerNodeNum()));
        if (machineId == 0) {
            L.info("master started");
            MasterNode.startMasterNode();
        } else {
            L.info("Worker Started " + machineId);
//            worker id start with 0, call mpi api need to add 1
            WorkerNode.startWorkerNode();
            int workerId = machineId - 1;
            AsyncWorker worker = new AsyncWorker(workerId, workerNum, 32);
            worker.startWorker();
        }
        L.info("process " + machineId + " exit.");
        MPI.Finalize();
        distAsyncEngine.run();
    }

}
