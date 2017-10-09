package socialite.async.codegen;

//AsyncRuntime(initSize, threadNum, dynamic, checkType, checkerInterval, threshold, cond) ::= <<

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.tables.TableInst;
import socialite.visitors.VisitorImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

public class AsyncRuntime implements Runnable {
    private static final Log L = LogFactory.getLog(AsyncRuntime.class);
    private BaseAsyncTable asyncTable;
    private final int threadNum;
    private ComputingThread[] computingThreads;
    private Checker checker;
    private AsyncConfig asyncConfig;
    private long checkerConsume;

    public AsyncRuntime(BaseAsyncTable asyncTable, TableInst[] recInstArr, TableInst[] edgeInstArr) {
        this.asyncTable = asyncTable;
        asyncConfig = AsyncConfig.get();
        threadNum = AsyncConfig.get().getThreadNum();
        loadData(recInstArr, edgeInstArr);
        L.info("Data Loaded size:" + asyncTable.getSize());
        createThreads();
    }

    private void loadData(TableInst[] initTableInstArr, TableInst[] edgeTableInstArr) {
        try {
            Method method;
            if (asyncConfig.isDynamic()) {
                //动态算法需要edge做连接，如prog4、9!>
                method = edgeTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
                for (TableInst tableInst : edgeTableInstArr) {
                    if (!tableInst.isEmpty()) {
                        method.invoke(tableInst, asyncTable.getEdgeVisitor());
                        //tableInst.clear();
                    }
                }
            }
            method = initTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
            for (TableInst tableInst : initTableInstArr) {
                if (!tableInst.isEmpty()) {
                    method.invoke(tableInst, asyncTable.getInitVisitor());
                    //tableInst.clear();
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    private void createThreads() {
        computingThreads = new ComputingThread[threadNum];
        checker = new Checker();
        checker.setPriority(Thread.MAX_PRIORITY);
        arrangeTask();
    }

    private void arrangeTask() {
        int blockSize = asyncTable.getSize() / threadNum;
        if (blockSize == 0) {
            L.warn("too many threads");
            blockSize = asyncTable.getSize();
            computingThreads[0] = new ComputingThread(0, 0, blockSize);
            return;
        }
        for (int tid = 0; tid < threadNum; tid++) {
            int start = tid * blockSize;
            int end = (tid + 1) * blockSize;
            if (tid == threadNum - 1)
                end = asyncTable.getSize();
            if (computingThreads[tid] == null) {
                computingThreads[tid] = new ComputingThread(tid, start, end);
            } else {
                computingThreads[tid].start = start;
                computingThreads[tid].end = end;
            }
        }
    }

    @Override
    public void run() {
        checker.start();
        Arrays.stream(computingThreads).filter(Objects::nonNull).forEach(ComputingThread::start);
        L.info("worker started");
        try {
//            for (ComputingThread worker : computingThreads)
//                if (worker != null)
//                    worker.join();
            checker.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        L.info("done elapsed:" + checker.stopWatch.getTime());
        L.info("checker consumed " + checkerConsume);
        L.info("checker thread exit");
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
            while (!checker.stop) {
                for (int k = start; k < end; k++) {
                    asyncTable.updateLockFree(k);
                }
            }
        }

        @Override
        public String toString() {
            return String.format("id: %d range: [%d, %d)", tid, start, end);
        }
    }


    private class Checker extends Thread {
        private boolean stop = false;
        StopWatch stopWatch;
        private final int CHECKER_INTERVAL = asyncConfig.getCheckInterval();
        StopWatch checkerSW;

        private Checker() {
            stopWatch = new StopWatch();
            checkerSW = new StopWatch();
        }

        private void done() {
            stop = true;
            stopWatch.stop();
        }

        @Override
        public void run() {
            stopWatch.start();
            while (true) {
                try {
                    checkerSW.reset();
                    checkerSW.start();
                    double sum = 0.0d;
                    if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA) {
                        if (asyncTable.accumulateDelta() instanceof Integer)
                            sum = ((Integer) asyncTable.accumulateDelta()) + 0.0d;
                        else if (asyncTable.accumulateDelta() instanceof Long)
                            sum = ((Long) asyncTable.accumulateDelta()) + 0.0d;
                        else if (asyncTable.accumulateDelta() instanceof Float)
                            sum = ((Float) asyncTable.accumulateDelta()) + 0.0d;
                        else if (asyncTable.accumulateDelta() instanceof Double)
                            sum = ((Double) asyncTable.accumulateDelta()) + 0.0d;
                        L.info("sum of delta: " + sum);
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE) {
                        if (asyncTable.accumulateValue() instanceof Integer)
                            sum = ((Integer) asyncTable.accumulateValue()) + 0.0d;
                        else if (asyncTable.accumulateValue() instanceof Long)
                            sum = ((Long) asyncTable.accumulateValue()) + 0.0d;
                        else if (asyncTable.accumulateValue() instanceof Float)
                            sum = ((Float) asyncTable.accumulateValue()) + 0.0d;
                        else if (asyncTable.accumulateValue() instanceof Double)
                            sum = ((Double) asyncTable.accumulateValue()) + 0.0d;
                        L.info("sum of delta: " + sum);
                    }
                    if (eval(sum)) {
                        done();
                        break;
                    }
                    checkerSW.stop();
                    checkerConsume += checkerSW.getTime();
                    if (asyncConfig.isDynamic())
                        arrangeTask();
                    Thread.sleep(CHECKER_INTERVAL);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private boolean eval(double val) {
            double threshold = asyncConfig.getThreshold();
            switch (asyncConfig.getCond()) {
                case G:
                    return val > threshold;
                case GE:
                    return val >= threshold;
                case E:
                    return val == threshold;
                case LE:
                    return val <= threshold;
                case L:
                    return val < threshold;
            }
            throw new UnsupportedOperationException();
        }
    }
}