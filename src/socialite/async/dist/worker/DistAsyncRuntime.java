package socialite.async.dist.worker;

import mpi.MPI;
import mpi.MPIException;
import mpi.Status;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.codegen.BaseDistAsyncTable;
import socialite.async.codegen.MessageTableBase;
import socialite.async.dist.MsgType;
import socialite.async.dist.Payload;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.util.SerializeTool;
import socialite.parser.Table;
import socialite.resource.DistTableSliceMap;
import socialite.resource.SRuntimeWorker;
import socialite.resource.TableInstRegistry;
import socialite.tables.TableInst;
import socialite.util.Loader;
import socialite.visitors.VisitorImpl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class DistAsyncRuntime implements Runnable {
    private static final Log L = LogFactory.getLog(DistAsyncRuntime.class);
    private BaseDistAsyncTable distAsyncTable;
    private int threadNum;
    private volatile boolean stop;
    private final int myWorkerId;
    private final int workerNum;
    private ComputingThread[] computingThreads;
    private CheckerThread checkerThread;
    private SendThread[] sendThreads;
    private ReceiveThread[] receiveThreads;
    private Payload payload;

    DistAsyncRuntime() {
        workerNum = MPI.COMM_WORLD.Size() - 1;
        myWorkerId = MPI.COMM_WORLD.Rank() - 1;
    }

    @Override
    public void run() {
        waitingCmd();
        if (loadData()) {//this worker is idle, stop
            createThreads();
            arrangeTask();
            startThreads();
            L.info(String.format("Worker %d all threads started.", myWorkerId));
        } else {//this worker is idle, only start checker
            checkerThread = new CheckerThread();
            checkerThread.start();
            try {
                checkerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void waitingCmd() {
        byte[] data = new byte[1024 * 1024];
        MPI.COMM_WORLD.Sendrecv(new int[]{SRuntimeWorker.getInst().getWorkerAddrMap().myIndex(), myWorkerId}, 0, 2, MPI.INT, AsyncMaster.ID, MsgType.REPORT_MYIDX.ordinal(),
                data, 0, data.length, MPI.BYTE, AsyncMaster.ID, MsgType.NOTIFY_INIT.ordinal());
        SerializeTool serializeTool = new SerializeTool.Builder().build();
        payload = serializeTool.fromBytes(data, Payload.class);
        AsyncConfig.set(payload.getAsyncConfig());
        L.info("RECV CMD NOTIFY_INIT CONFIG:" + AsyncConfig.get());
        threadNum = AsyncConfig.get().getThreadNum();
    }

    private boolean loadData() {
        Loader.loadFromBytes(payload.getByteCodes());
        Class<?> messageTableClass = Loader.forName("socialite.async.codegen.MessageTable");
        Class<?> distAsyncTableClass = Loader.forName("socialite.async.codegen.DistAsyncTable");
        try {
            SRuntimeWorker runtimeWorker = SRuntimeWorker.getInst();
            TableInstRegistry tableInstRegistry = runtimeWorker.getTableRegistry();
            Map<String, Table> tableMap = runtimeWorker.getTableMap();
            DistTableSliceMap sliceMap = runtimeWorker.getSliceMap();
            TableInst[] initTableInstArr = tableInstRegistry.getTableInstArray(tableMap.get("InitTable").id());
            TableInst[] edgeTableInstArr = tableInstRegistry.getTableInstArray(tableMap.get(payload.getEdgeTableName()).id());

            //static, int type key
            int indexForTableid;
            if (AsyncConfig.get().isDynamic()) {
                TableInst edgeInst = Arrays.stream(edgeTableInstArr).filter(tableInst -> !tableInst.isEmpty()).findFirst().orElse(null);
                if (edgeInst == null) {
                    L.warn("worker " + myWorkerId + " has no job");
                    return false;
                }
                Method method = edgeInst.getClass().getMethod("tableid");
                indexForTableid = (Integer) method.invoke(edgeInst);
                //public DistAsyncTable(Class\<?> messageTableClass, DistTableSliceMap sliceMap, int indexForTableId) {
                Constructor constructor = distAsyncTableClass.getConstructor(messageTableClass.getClass(), DistTableSliceMap.class, int.class, Map.class);
                distAsyncTable = (BaseDistAsyncTable) constructor.newInstance(messageTableClass, sliceMap, indexForTableid, payload.getMyIdxWorkerIdMap());
                //动态算法需要edge做连接，如prog4、9!>
                method = edgeTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
                for (TableInst tableInst : edgeTableInstArr) {
                    if (!tableInst.isEmpty()) {
                        method.invoke(tableInst, distAsyncTable.getEdgeVisitor());
                        //tableInst.clear();
                    }
                }
            } else {
                TableInst initTableInst = Arrays.stream(initTableInstArr).filter(tableInst -> !tableInst.isEmpty()).findFirst().orElse(null);
                if (initTableInst == null) {
                    L.warn("worker " + myWorkerId + " has no job");
                    return false;
                }
                Method method = initTableInst.getClass().getMethod("tableid");
                indexForTableid = (Integer) method.invoke(initTableInst);
                Field baseField = initTableInstArr[0].getClass().getDeclaredField("base");
                baseField.setAccessible(true);
                int base = baseField.getInt(Arrays.stream(initTableInstArr).filter(tableInst -> !tableInst.isEmpty()).findFirst().orElse(null));
                //public DistAsyncTable(Class\<?> messageTableClass, DistTableSliceMap sliceMap, int indexForTableId, int base) {
                Constructor constructor = distAsyncTableClass.getConstructor(messageTableClass.getClass(), DistTableSliceMap.class, int.class, Map.class, int.class);
                distAsyncTable = (BaseDistAsyncTable) constructor.newInstance(messageTableClass, sliceMap, indexForTableid, payload.getMyIdxWorkerIdMap(), base);
            }


            Method method = initTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
            for (TableInst tableInst : initTableInstArr) {
                if (!tableInst.isEmpty()) {
                    method.invoke(tableInst, distAsyncTable.getInitVisitor());
                    //tableInst.clear();
                }
            }

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        L.info("WorkerId " + myWorkerId + " Data Loaded size:" + distAsyncTable.getSize());
        return true;
    }

    private void createThreads() {
        computingThreads = new ComputingThread[threadNum];
        checkerThread = new CheckerThread();
        sendThreads = new SendThread[workerNum];
        receiveThreads = new ReceiveThread[workerNum];
        for (int wid = 0; wid < workerNum; wid++) {
            if (wid == myWorkerId) continue; //skip me
            sendThreads[wid] = new SendThread(wid);
            receiveThreads[wid] = new ReceiveThread(wid);
        }
    }

    private void startThreads() {
        Arrays.stream(computingThreads).filter(Objects::nonNull).forEach(Thread::start);
        Arrays.stream(receiveThreads).filter(Objects::nonNull).forEach(Thread::start);
        Arrays.stream(sendThreads).filter(Objects::nonNull).forEach(Thread::start);
        checkerThread.start();
        try {
            checkerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void arrangeTask() {
        int blockSize = distAsyncTable.getSize() / threadNum;
        if (blockSize == 0) {
            L.warn("too many threads");
            blockSize = distAsyncTable.getSize();
            computingThreads[0] = new ComputingThread(0, 0, blockSize);
            return;
        }

        for (int tid = 0; tid < threadNum; tid++) {
            int start = tid * blockSize;
            int end = (tid + 1) * blockSize;
            if (tid == threadNum - 1)
                end = distAsyncTable.getSize();
            if (computingThreads[tid] == null) {
                computingThreads[tid] = new ComputingThread(tid, start, end);
            } else {
                computingThreads[tid].start = start;
                computingThreads[tid].end = end;
            }
        }
    }

    class ComputingThread extends Thread {
        int start;
        int end;
        int tid;

        private ComputingThread(int tid, int start, int end) {
            this.tid = tid;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            while (!stop) {
                for (int k = start; k < end; k++) {
//                    L.info("Worker " + myWorkerId + " update " + k);
                    distAsyncTable.updateLockFree(k);
                }
            }
        }

        @Override
        public String toString() {
            return String.format("id: %d range: [%d, %d)", tid, start, end);
        }
    }

    class SendThread extends Thread {
        private int sendToWorkerId;
        private SerializeTool serializeTool;

        private SendThread(int sendToWorkerId) {
            this.sendToWorkerId = sendToWorkerId;
            serializeTool = new SerializeTool.Builder()
                    .setSerializeTransient(true) //!!!!!!!!!!AtomicDouble's value field is transient
                    .build();
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    byte[] data = distAsyncTable.getSendableMessageTableBytes(sendToWorkerId, serializeTool);
                    MPI.COMM_WORLD.Send(data, 0, data.length, MPI.BYTE, sendToWorkerId + 1, MsgType.MESSAGE_TABLE.ordinal());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class ReceiveThread extends Thread {
        private SerializeTool serializeTool;
        private int recvFromWorkerId;
        private Class<?> klass;

        private ReceiveThread(int recvFromWorkerId) {
            this.recvFromWorkerId = recvFromWorkerId;
            serializeTool = new SerializeTool.Builder()
                    .setSerializeTransient(true)
                    .build();
            klass = Loader.forName("socialite.async.codegen.MessageTable");
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    applyBufferToAsyncTable();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void applyBufferToAsyncTable() throws MPIException, InterruptedException {
            Status status = MPI.COMM_WORLD.Probe(recvFromWorkerId + 1, MsgType.MESSAGE_TABLE.ordinal());
            int size = status.Get_count(MPI.BYTE);
            int source = status.source;
//            L.info(String.format("worker %d probe %d size %d MB", recvFromWorkerId + 1, source, size / 1024 / 1024));
            byte[] data = new byte[size];
//            L.info("applyBufferToAsyncTable");
            MPI.COMM_WORLD.Recv(data, 0, size, MPI.BYTE, source, MsgType.MESSAGE_TABLE.ordinal());
//            L.info(String.format("Machine %d <---- %d", workerId + 1, source));
            MessageTableBase messageTable = (MessageTableBase) serializeTool.fromBytes1(data, klass);
            distAsyncTable.applyBuffer(messageTable);
        }
    }

    private class CheckerThread extends Thread {
        double[] partialValue;
        boolean[] feedback;

        private CheckerThread() {
            partialValue = new double[1];
            feedback = new boolean[1];
        }

        @Override
        public void run() {
            while (true) {
                MPI.COMM_WORLD.Recv(new byte[1], 0, 1, MPI.BYTE, AsyncMaster.ID, MsgType.REQUIRE_TERM_CHECK.ordinal());
                double sum = 0;
                if (distAsyncTable != null) {//null indicate this worker is idle
                    if (AsyncConfig.get().getCheckType() == AsyncConfig.CheckerType.DELTA) {
                        if (distAsyncTable.accumulateDelta() instanceof Integer)
                            sum = ((Integer) distAsyncTable.accumulateDelta()) + 0.0d;
                        else if (distAsyncTable.accumulateDelta() instanceof Long)
                            sum = ((Long) distAsyncTable.accumulateDelta()) + 0.0d;
                        else if (distAsyncTable.accumulateDelta() instanceof Float)
                            sum = ((Float) distAsyncTable.accumulateDelta()) + 0.0d;
                        else if (distAsyncTable.accumulateDelta() instanceof Double)
                            sum = ((Double) distAsyncTable.accumulateDelta()) + 0.0d;
                    } else if (AsyncConfig.get().getCheckType() == AsyncConfig.CheckerType.VALUE) {
                        if (distAsyncTable.accumulateValue() instanceof Integer)
                            sum = ((Integer) distAsyncTable.accumulateValue()) + 0.0d;
                        else if (distAsyncTable.accumulateValue() instanceof Long)
                            sum = ((Long) distAsyncTable.accumulateValue()) + 0.0d;
                        else if (distAsyncTable.accumulateValue() instanceof Float)
                            sum = ((Float) distAsyncTable.accumulateValue()) + 0.0d;
                        else if (distAsyncTable.accumulateValue() instanceof Double)
                            sum = ((Double) distAsyncTable.accumulateValue()) + 0.0d;
                    }
                }
                partialValue[0] = sum;
                L.info("Worker " + myWorkerId + " sum of delta: " + sum);
                MPI.COMM_WORLD.Sendrecv(partialValue, 0, 1, MPI.DOUBLE, AsyncMaster.ID, MsgType.TERM_CHECK_PARTIAL_VALUE.ordinal(),
                        feedback, 0, 1, MPI.BOOLEAN, AsyncMaster.ID, MsgType.TERM_CHECK_FEEDBACK.ordinal());
                if (feedback[0]) {
                    done();
                    break;
                }
            }
        }

        private void done() {
            stop = true;
            saveResult();
            System.exit(0);
        }
    }

    private void saveResult() {
        if (distAsyncTable == null) return;
        L.info("worker " + myWorkerId + " saving...");
        distAsyncTable.iterate(new MyVisitorImpl() {

            @Override
            public boolean visit(int a1, double a2, double a3) {
                System.out.println(a1 + " " + a2 + " " + a3);
                return true;
            }

            //CC
            @Override
            public boolean visit(int a1, int a2, int a3) {
                System.out.println(a1 + " " + a2 + " " + a3);
                return true;
            }

            //COUNT PATH IN DAG
            @Override
            public boolean visit(Object a1, int a2, int a3) {
                System.out.println(a1 + " " + a2 + " " + a3);
                return true;
            }

            //PARTY
            @Override
            public boolean visit(int a1) {
                System.out.println(a1);
                return true;
            }
        });
    }
}
