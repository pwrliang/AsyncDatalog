package socialite.async.codegen;

//AsyncRuntime(initSize, threadNum, dynamic, checkType, checkerInterval, threshold, cond) ::= <<

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.tables.TableInst;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public abstract class BaseAsyncRuntime implements Runnable {
    protected static final Log L = LogFactory.getLog(BaseAsyncRuntime.class);
    private volatile boolean stop;
    protected ComputingThread[] computingThreads;
    protected CheckThread checkThread;
    protected BaseAsyncTable asyncTable;

    protected abstract boolean loadData(TableInst[] initTableInstArr, TableInst[] edgeTableInstArr);

    protected abstract void createThreads();

    protected boolean isStop() {
        return stop;
    }

    public BaseAsyncTable getAsyncTable() {
        return asyncTable;
    }

    protected class ComputingThread extends Thread {
        private volatile int start;
        private volatile int end;
        private int tid;
        double[] deltaSample;
        final double SAMPLE_RATE;
        final double SCHEDULE_PORTION;
        boolean assigned;
        private AsyncConfig asyncConfig;
        private ThreadLocalRandom randomGenerator;

        public ComputingThread(int tid) {
            this.tid = tid;
            asyncConfig = AsyncConfig.get();
            randomGenerator = ThreadLocalRandom.current();
            SAMPLE_RATE = asyncConfig.getSampleRate();
            SCHEDULE_PORTION = asyncConfig.getSchedulePortion();
        }


        @Override
        public void run() {
            while (!stop) {
                if (!assigned || asyncConfig.isDynamic()) {
                    arrangeTask();
                    assigned = true;
                }

                //empty thread, sleep to reduce CPU race
                if (start == end) {
                    try {
                        Thread.sleep(10);
                        continue;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                double threshold = 0;
                if (asyncConfig.getPriorityType() != AsyncConfig.PriorityType.NONE) {
                    for (int i = 0; i < deltaSample.length; i++) {
                        int ind = randomGenerator.nextInt(start, end);
                        if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.TYPE1)
                            deltaSample[i] = asyncTable.getDelta(ind);
                        else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.TYPE2) {
                            double value = asyncTable.getValue(ind);
                            double delta = asyncTable.getDelta(ind);
                            deltaSample[i] = value - Math.min(value, delta);
                        } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.TYPE3) {
                            double value = asyncTable.getValue(ind);
                            double delta = asyncTable.getDelta(ind);
                            deltaSample[i] = value - Math.max(value, delta);
                        }
                    }
                    Arrays.sort(deltaSample);
                    int cutIndex = (int) (deltaSample.length * (1 - SCHEDULE_PORTION));
                    threshold = deltaSample[cutIndex];
                }

                if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.NONE) {
                    for (int k = start; k < end; k++) {
                        asyncTable.updateLockFree(k);
                    }
                } else {
                    int processed = 0;
                    for (int k = start; k < end; k++) {
                        double f = asyncTable.getDelta(k);
                        if (f >= threshold) {
                            processed++;
                            asyncTable.updateLockFree(k);
                        }
                    }
                    if (tid == 0) {
//                        L.info("processed " + (float) processed / (end - start) * 100);
                    }
                }
            }
        }

        private void arrangeTask() {
            int threadNum = AsyncConfig.get().getThreadNum();
            int size = asyncTable.getSize();
            int blockSize = size / threadNum;
            if (blockSize == 0) {
                L.warn("too many threads asynctable size " + size);
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
                computingThreads[tid].start = start;
                computingThreads[tid].end = end;
                if (asyncConfig.getPriorityType() != AsyncConfig.PriorityType.NONE) {
                    deltaSample = new double[(int) ((end - start) * SAMPLE_RATE)];
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

        protected CheckThread() {
            stopWatch = new StopWatch();
        }

        @Override
        public void run() {
            stopWatch.start();
        }

        protected void done() {
            stop = true;
            stopWatch.stop();
            L.info("done elapsed:" + stopWatch.getTime());
            L.info("checker thread exit");
        }

    }

    public static boolean eval(double val) {
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