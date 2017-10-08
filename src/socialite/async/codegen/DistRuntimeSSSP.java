package socialite.async.codegen;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.dist.worker.DistAsyncRuntime;
import socialite.tables.TableInst;
import socialite.visitors.VisitorImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DistRuntimeSSSP {
    private static final Log L = LogFactory.getLog(DistAsyncRuntime.class);
    BaseAsyncTable distAsyncTable;
//    private DistAsyncRuntime.ComputingThread[] computingThreads;
//    private DistAsyncRuntime.SendThread[] sendThreads;
//    private DistAsyncRuntime.ReceiveThread[] receiveThreads;
//    private DistAsyncRuntime.CheckerThread checkerThread;
    private volatile boolean stop;//notify computing threads to stop

    //////////////////////
    private static final int INIT_MESSAGE_TABLE_SIZE = 10000;
    private static final int INIT_ASYNC_TABLE_SIZE = 100000;
    private static final int MESSAGE_TABLE_UPDATE_THRESHOLD = 5000000;
    /////////////////////
    public final int workerId;
    private final int workerNum;
    private int threadNum;
    public DistRuntimeSSSP(int workerId, int workerNum, int threadNum) {
        this.workerId = workerId;
        this.workerNum = workerNum;
        this.threadNum = threadNum;
    }

    private void loadData(TableInst[] initTableInstArr, TableInst[] edgeTableInstArr) {
        try {
            Method method;
            method = edgeTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
            for (TableInst tableInst : edgeTableInstArr) {
                if (!tableInst.isEmpty()) {
                    method.invoke(tableInst, distAsyncTable.getEdgeVisitor());
                    //tableInst.clear();
                }
            }


            method = initTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
            for (TableInst tableInst : initTableInstArr) {
                if (!tableInst.isEmpty()) {
                    method.invoke(tableInst, distAsyncTable.getInitVisitor());
                    //tableInst.clear();
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
