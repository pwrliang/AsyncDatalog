package socialite.async;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.dist.worker.AsyncWorker;
import socialite.dist.master.MasterNode;
import socialite.dist.worker.WorkerNode;
import socialite.engine.Config;
import socialite.util.SociaLiteException;

public class Entry {
    private static final Log L = LogFactory.getLog(Entry.class);

    //-jar /opt/mpj-v0_44/lib/starter.jar -machinesfile /home/gengl/socialite-mod/machines -np 2 -dev niodev -Dlog4j.configuration=file:/home/gengl/socialite-mod/conf/log4j.properties
    public static void main(String[] args) throws InterruptedException {
        MPI.Init(args);
        int machineNum = MPI.COMM_WORLD.Size();
        int machineId = MPI.COMM_WORLD.Rank();
        int workerNum = machineNum - 1;
        System.setProperty("socialite.worker.num", workerNum + "");
        if (machineNum - 1 != Config.getWorkerNodeNum())
            throw new SociaLiteException(String.format("MPI Workers (%d)!= Socialite Workers (%d)", workerNum, Config.getWorkerNodeNum()));
        if (machineId == 0) {
            L.info("master started");
            MasterNode.startMasterNode();
            AsyncMaster asyncMaster = new AsyncMaster(args[args.length - 1], workerNum);
            asyncMaster.startMaster();
        } else {
            L.info("Worker Started " + machineId);
//            worker id start with 0, call mpi api need to add 1
            WorkerNode.startWorkerNode();
            int workerId = machineId - 1;
            AsyncWorker worker = new AsyncWorker(workerId, workerNum, 1);
            worker.startWorker();
        }
        L.info("process " + machineId + " exit.");
        MPI.Finalize();
    }
}
