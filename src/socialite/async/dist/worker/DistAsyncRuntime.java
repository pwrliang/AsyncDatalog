package socialite.async.dist.worker;

import mpi.MPI;
import mpi.MPIException;
import mpi.Status;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.codegen.AsyncRuntimeBase;
import socialite.async.codegen.BaseAsyncTable;
import socialite.async.dist.ds.DistAsyncTable;
import socialite.async.dist.ds.MessageTable;
import socialite.async.dist.ds.MsgType;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.util.SerializeTool;
import socialite.parser.Table;
import socialite.resource.SRuntimeWorker;
import socialite.resource.TableInstRegistry;
import socialite.tables.TableInst;
import socialite.visitors.VisitorImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;

//this class needs dynamic generate future
public class DistAsyncRuntime extends AsyncRuntimeBase {
    private static final Log L = LogFactory.getLog(DistAsyncRuntime.class);
    private static final int INIT_MESSAGE_TABLE_SIZE = 10000;
    private static final int INIT_ASYNC_TABLE_SIZE = 100000;
    private static final int MESSAGE_TABLE_UPDATE_THRESHOLD = 5000000;
    public final int workerId;
    private final int workerNum;
    private int threadNum;
    private DistAsyncTable distAsyncTable;
    private ComputingThread[] computingThreads;
    private SendThread[] sendThreads;
    private ReceiveThread[] receiveThreads;
    private CheckerThread checkerThread;
    private volatile boolean stop;//notify computing threads to stop

    public DistAsyncRuntime(int workerId, int workerNum, int threadNum) {
        this.workerId = workerId;
        this.workerNum = workerNum;
        this.threadNum = threadNum;
    }

    @Override
    public void run() {
        initData();
        createThreads();
        arrangeTask();
        startThreads();
        L.info(String.format("Worker %d all threads started.", workerId));
    }


    private void initData() {
        MPI.COMM_WORLD.Recv(new byte[1], 0, 1, MPI.BYTE, AsyncMaster.ID, MsgType.NOTIFY_INIT.ordinal());
        //init keys
        distAsyncTable = new DistAsyncTable(workerNum, workerId, INIT_ASYNC_TABLE_SIZE, INIT_MESSAGE_TABLE_SIZE);
        TableInstRegistry tableInstRegistry = SRuntimeWorker.getInst().getTableRegistry();
        Map<String, Table> tableMap = SRuntimeWorker.getInst().getTableMap();
        Table initTable = tableMap.get("Middle");
        distAsyncTable.setTableId(initTable.id());
        distAsyncTable.setSliceMap(SRuntimeWorker.getInst().getSliceMap());


        TableInst[] tableInsts = tableInstRegistry.getTableInstArray(initTable.id());
        try {
            Method method = tableInsts[0].getClass().getMethod("iterate", VisitorImpl.class);
            for (TableInst tableInst : tableInsts) {
                if (!tableInst.isEmpty())
                    method.invoke(tableInst, distAsyncTable.getMiddleVisitor());
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        L.info("machine " + workerId + " dataloaded " + distAsyncTable.getSize());
//        distAsyncTable.display();

    }

    private void createThreads() {
        computingThreads = new ComputingThread[threadNum];
        checkerThread = new CheckerThread();
        sendThreads = new SendThread[workerNum];
        receiveThreads = new ReceiveThread[workerNum];
        for (int wid = 0; wid < workerNum; wid++) {
            if (wid == workerId) continue; //skip me
            sendThreads[wid] = new SendThread(wid, distAsyncTable.getMessageTableSelector());
            receiveThreads[wid] = new ReceiveThread(wid);
        }

    }

    private void startThreads() {
        Arrays.stream(receiveThreads).filter(Objects::nonNull).forEach(Thread::start);
        Arrays.stream(sendThreads).filter(Objects::nonNull).forEach(Thread::start);
        checkerThread.start();
        Arrays.stream(computingThreads).forEach(Thread::start);
        try {
            checkerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void arrangeTask() {
        int blockSize = distAsyncTable.getSize() / threadNum;
        if (blockSize == 0 && distAsyncTable.getSize() > 0) {
            L.error("too many threads");
            System.exit(0);
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

    @Override
    public BaseAsyncTable getAsyncTable() {
        return distAsyncTable;
    }

    private void saveResult() {
        distAsyncTable.iterate(new MyVisitorImpl() {
            @Override
            public boolean visit(int a1, double a2, double a3) {
                L.info("machine: " + (workerId + 1) + "\t" + a1 + "\t" + a2 + "\t" + a3);
                return false;
            }
        });
    }

    private class ComputingThread extends Thread {
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
            try {
                while (!stop) {
                    for (int localInd = start; localInd < end; localInd++) {
                        distAsyncTable.updateLockFree(localInd);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public String toString() {
            return String.format("id: %d range: [%d, %d)", tid, start, end);
        }
    }

    private class SendThread extends Thread {
        MessageTable[] messageTableAndBackup;
        AtomicIntegerArray messageTableSelector;
        private SerializeTool serializeTool;
        private int sendToWorkerId;

        private SendThread(int sendToWorkerId, AtomicIntegerArray messageTableSelector) {
            this.sendToWorkerId = sendToWorkerId;
            this.messageTableSelector = messageTableSelector;
            serializeTool = new SerializeTool.Builder()
                    .setSerializeTransient(true) //!!!!!!!!!!AtomicDouble's value field is transient
                    .build();
            messageTableAndBackup = distAsyncTable.getMessageTables(sendToWorkerId);
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    sendMessageTable();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void sendMessageTable() throws InterruptedException {
            int tableInd = messageTableSelector.get(sendToWorkerId);
            MessageTable messageTable = messageTableAndBackup[tableInd];

            while (messageTable.getUpdateTimes() < MESSAGE_TABLE_UPDATE_THRESHOLD)//wait until collected enough data
                Thread.sleep(100);
            messageTableSelector.set(sendToWorkerId, tableInd == 0 ? 1 : 0);//switch to backup message table


            byte[] data = serializeTool.toBytes(messageTable);
            MPI.COMM_WORLD.Send(data, 0, data.length, MPI.BYTE, sendToWorkerId + 1, MsgType.MESSAGE_TABLE.ordinal());
            messageTable.resetDelta();//clear delta !!!! if interval checker is enable, should lock it
//            L.info(String.format("Machine %d ----> Machine %d", workerId + 1, sendToWorkerId + 1));
        }
    }

    private class ReceiveThread extends Thread {
        private SerializeTool serializeTool;
        private int recvFromWorkerId;

        private ReceiveThread(int recvFromWorkerId) {
            this.recvFromWorkerId = recvFromWorkerId;
            serializeTool = new SerializeTool.Builder()
                    .setSerializeTransient(true)
                    .build();
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
            MessageTable messageTable = serializeTool.fromBytes(data, MessageTable.class);
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
            while (!stop) {
                MPI.COMM_WORLD.Recv(new byte[1], 0, 1, MPI.BYTE, AsyncMaster.ID, MsgType.REQUIRE_TERM_CHECK.ordinal());
                double sum = (Double) distAsyncTable.accumulateValue();
                partialValue[0] = sum;
                L.info("Worker " + workerId + " sum of delta: " + sum);
                MPI.COMM_WORLD.Sendrecv(partialValue, 0, 1, MPI.DOUBLE, AsyncMaster.ID, MsgType.TERM_CHECK_PARTIAL_VALUE.ordinal(),
                        feedback, 0, 1, MPI.BOOLEAN, AsyncMaster.ID, MsgType.TERM_CHECK_FEEDBACK.ordinal());
                if (feedback[0]) {
                    done();
                    break;
                }
            }
        }

        private void done() {
            DistAsyncRuntime.this.stop = true;
            saveResult();
        }
    }
}
