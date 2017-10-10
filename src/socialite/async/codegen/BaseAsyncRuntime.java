package socialite.async.codegen;

//AsyncRuntime(initSize, threadNum, dynamic, checkType, checkerInterval, threshold, cond) ::= <<

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.tables.TableInst;

public abstract class BaseAsyncRuntime implements Runnable {
    protected static final Log L = LogFactory.getLog(BaseAsyncRuntime.class);
    private volatile boolean stop;
    protected ComputingThread[] computingThreads;
    protected CheckThread checkThread;
    protected BaseAsyncTable asyncTable;

    protected abstract boolean loadData(TableInst[] initTableInstArr, TableInst[] edgeTableInstArr);

    protected abstract void createThreads();

    protected void arrangeTask() {
        int threadNum = AsyncConfig.get().getThreadNum();
        int size = asyncTable.getSize();
        int blockSize = size / threadNum;
        if (blockSize == 0) {
            L.warn("too many threads");
            blockSize = size;
        }

        for (int tid = 0; tid < threadNum; tid++) {
            int start = tid * blockSize;
            int end = (tid + 1) * blockSize;
            if (tid == threadNum - 1)//last thread, assign all
                end = size;
            if (start >= size) {//assign empty tasks
                start = 0;
                end = 0;
            } else if (end > size || tid == threadNum - 1) {//block < lastThread's tasks or block > ~
                end = size;
            }
            if (computingThreads[tid] == null) {
                computingThreads[tid] = new ComputingThread(tid, start, end);
            } else {
                computingThreads[tid].start = start;
                computingThreads[tid].end = end;
            }
        }
    }

    protected boolean isStop() {
        return stop;
    }

    protected class ComputingThread extends Thread {
        private volatile int start;
        private volatile int end;
        private int tid;

        private ComputingThread(int tid, int start, int end) {
            this.tid = tid;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            while (!stop) {
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

    protected abstract class CheckThread extends Thread {
        protected StopWatch stopWatch;
        protected final int CHECKER_INTERVAL = AsyncConfig.get().getCheckInterval();
        protected StopWatch checkerSW;
        protected int checkerConsume;

        protected CheckThread() {
            stopWatch = new StopWatch();
            checkerSW = new StopWatch();
        }

        protected void done() {
            stop = true;
            stopWatch.stop();
            L.info("done elapsed:" + stopWatch.getTime());
            L.info("checker consumed " + checkerConsume);
            L.info("checker thread exit");
        }

//        @Override
//        public void run() {
//            stopWatch.start();
//            while (true) {
//                try {
//                    checkerSW.reset();
//                    checkerSW.start();
//                    double sum = 0.0d;
//                    if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA) {
//                        if (asyncTable.accumulateDelta() instanceof Integer)
//                            sum = ((Integer) asyncTable.accumulateDelta()) + 0.0d;
//                        else if (asyncTable.accumulateDelta() instanceof Long)
//                            sum = ((Long) asyncTable.accumulateDelta()) + 0.0d;
//                        else if (asyncTable.accumulateDelta() instanceof Float)
//                            sum = ((Float) asyncTable.accumulateDelta()) + 0.0d;
//                        else if (asyncTable.accumulateDelta() instanceof Double)
//                            sum = ((Double) asyncTable.accumulateDelta()) + 0.0d;
//                        L.info("sum of delta: " + sum);
//                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE) {
//                        if (asyncTable.accumulateValue() instanceof Integer)
//                            sum = ((Integer) asyncTable.accumulateValue()) + 0.0d;
//                        else if (asyncTable.accumulateValue() instanceof Long)
//                            sum = ((Long) asyncTable.accumulateValue()) + 0.0d;
//                        else if (asyncTable.accumulateValue() instanceof Float)
//                            sum = ((Float) asyncTable.accumulateValue()) + 0.0d;
//                        else if (asyncTable.accumulateValue() instanceof Double)
//                            sum = ((Double) asyncTable.accumulateValue()) + 0.0d;
//                        L.info("sum of delta: " + sum);
//                    }
//                    if (eval(sum)) {
//                        done();
//                        break;
//                    }
//                    checkerSW.stop();
//                    checkerConsume += checkerSW.getTime();
//                    if (asyncConfig.isDynamic())
//                        arrangeTask();
//                    Thread.sleep(CHECKER_INTERVAL);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }

        protected boolean eval(double val) {



            AsyncConfig asyncConfig = AsyncConfig.get();
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