package socialite.async;

import mpi.MPI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.dist.worker.AsyncWorker;
import socialite.async.engine.LocalAsyncEngine;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;
import socialite.dist.worker.WorkerNode;
import socialite.engine.Config;
import socialite.util.SociaLiteException;

public class Entry {
    private static final Log L = LogFactory.getLog(Entry.class);

    //-jar /opt/mpj-v0_44/lib/starter.jar -machinesfile /home/gengl/socialite-mod/machines -np 2 -dev niodev -Dlog4j.configuration=file:/home/gengl/socialite-mod/conf/log4j.properties
    public static void main(String[] args) throws InterruptedException {
        AsyncConfig.parse(TextUtils.readText(args[0]));
        AsyncConfig asyncConfig = AsyncConfig.get();
        if(asyncConfig.getEngineType()== AsyncConfig.EngineType.STANDALONE) {
            //prog9 volatile problem
            LocalAsyncEngine localAsyncEngine = new LocalAsyncEngine(asyncConfig.getDatalogProg());
            localAsyncEngine.run();
        }else {
            AsyncMaster asyncMaster = new AsyncMaster(asyncConfig.getDatalogProg());
            asyncMaster.startMaster(args);
        }
    }
}
