package socialite.async;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.dist.worker.AsyncWorker;
import socialite.async.engine.LocalAsyncEngine;
import socialite.async.util.MPIUtils;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;
import socialite.dist.worker.WorkerNode;
import socialite.engine.Config;
import socialite.util.SociaLiteException;

public class Entry {
    private static final Log L = LogFactory.getLog(Entry.class);

    //-jar /opt/mpj-v0_44/lib/starter.jar -machinesfile /home/gengl/socialite-mod/machines -np 2 -dev niodev -Dlog4j.configuration=file:/home/gengl/socialite-mod/conf/log4j.properties
    public static void main(String[] args) throws InterruptedException {
        if (MPIUtils.inMPIEnv()) {
            MPI.Init(args);
            int machineNum = MPI.COMM_WORLD.Size();
            int machineId = MPI.COMM_WORLD.Rank();
            int workerNum = machineNum - 1;
            L.info("Machine " + machineId + " Xmx " + Runtime.getRuntime().maxMemory() / 1024 / 1024);
            if (machineNum - 1 != Config.getWorkerNodeNum())
                throw new SociaLiteException(String.format("MPI Workers (%d)!= Socialite Workers (%d)", workerNum, Config.getWorkerNodeNum()));
            if (machineId == 0) {
                AsyncConfig.parse(TextUtils.readText(args[args.length - 1]));
                L.info("master started");
                MasterNode.startMasterNode();
                AsyncMaster asyncMaster = new AsyncMaster(AsyncConfig.get().getDatalogProg());
                asyncMaster.startMaster();
            } else {
                L.info("Worker Started " + machineId);
                WorkerNode.startWorkerNode();
                AsyncWorker worker = new AsyncWorker();
                worker.startWorker();
            }
            L.info("process " + machineId + " exit.");
            MPI.Finalize();
        } else {
            AsyncConfig.parse(TextUtils.readText(args[0]));
            AsyncConfig asyncConfig = AsyncConfig.get();
            LocalAsyncEngine localAsyncEngine = new LocalAsyncEngine(asyncConfig.getDatalogProg());
            localAsyncEngine.run();
        }
    }
}
