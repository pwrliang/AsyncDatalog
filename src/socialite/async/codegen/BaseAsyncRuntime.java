package socialite.async.codegen;

//AsyncRuntime(initSize, threadNum, dynamic, checkType, checkerInterval, threshold, cond) ::= <<

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.tables.TableInst;

import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseAsyncRuntime implements Runnable {
    protected static final Log L = LogFactory.getLog(BaseAsyncRuntime.class);
    private volatile boolean stop;
    protected ComputingThread[] computingThreads;
    protected CheckThread checkThread;
    protected BaseAsyncTable asyncTable;
    protected CyclicBarrier barrier;
    protected AtomicInteger updateCounter;

    protected BaseAsyncRuntime() {
        updateCounter = new AtomicInteger();
    }

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
        final double DELTA_FILTER;
        boolean assigned;
        private AsyncConfig asyncConfig;
        private ThreadLocalRandom randomGenerator;

        public ComputingThread(int tid) {
            this.tid = tid;
            asyncConfig = AsyncConfig.get();
            randomGenerator = ThreadLocalRandom.current();
            SAMPLE_RATE = asyncConfig.getSampleRate();
            SCHEDULE_PORTION = asyncConfig.getSchedulePortion();
            DELTA_FILTER = asyncConfig.getDeltaFilter();
        }


        @Override
        public void run() {
            try {
                while (!stop) {
                    if (!assigned || asyncConfig.isDynamic()) {
                        arrangeTask();
                        assigned = true;
                    }

                    //empty thread, sleep to reduce CPU race
                    if (start == end) {
                        Thread.sleep(10);
                        if (barrier != null) barrier.await();
                        continue;
                    }

                    double threshold = 0;
                    if (asyncConfig.getPriorityType() != AsyncConfig.PriorityType.NONE) {
                        for (int i = 0; i < deltaSample.length; i++) {
                            int ind = randomGenerator.nextInt(start, end);
                            double delta = asyncTable.getDelta(ind);
                            if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.SUM_COUNT)
                                deltaSample[i] = delta;
                            else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.MIN) {
                                double value = asyncTable.getValue(ind);
                                deltaSample[i] = value - Math.min(value, delta);
                            } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.MAX) {
                                double value = asyncTable.getValue(ind);
                                deltaSample[i] = value - Math.max(value, delta);
                            }
                        }
                        if (deltaSample.length == 0) {//no sample, schedule all
                            threshold = -Double.MAX_VALUE;
                        } else {
                            Arrays.sort(deltaSample);
                            int cutIndex = (int) (deltaSample.length * (1 - SCHEDULE_PORTION));
                            if (cutIndex == 0)
                                threshold = -Double.MAX_VALUE;
                            else
                                threshold = deltaSample[cutIndex];

                        }
                    }

                    if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.NONE) {
                        for (int k = start; k < end; k++) {
                            if (asyncTable.getDelta(k) < DELTA_FILTER) continue;
                            if (asyncConfig.isSync()) {
                                if (asyncTable.updateLockFree(k, checkThread.iter)) updateCounter.addAndGet(1);
                            } else {
                                if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                            }
                        }
                    } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.SUM_COUNT) {
                        for (int k = start; k < end; k++) {
                            if (asyncTable.getDelta(k) < DELTA_FILTER) continue;
                            double delta = asyncTable.getDelta(k);
                            if (delta >= threshold) {
                                if (asyncConfig.isSync()) {
                                    if (asyncTable.updateLockFree(k, checkThread.iter)) updateCounter.addAndGet(1);
                                } else {
                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                                }
                            }
                        }
                    } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.MIN) {
                        for (int k = start; k < end; k++) {
                            if (asyncTable.getDelta(k) < DELTA_FILTER) continue;
                            double delta = asyncTable.getDelta(k);
                            double value = asyncTable.getValue(k);
                            double f = value - Math.min(value, delta);
                            if (f >= threshold) {
                                if (asyncConfig.isSync()) {
                                    if (asyncTable.updateLockFree(k, checkThread.iter)) updateCounter.addAndGet(1);
                                } else {
                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                                }
                            }
                        }
                    } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.MAX) {
                        for (int k = start; k < end; k++) {
                            if (asyncTable.getDelta(k) < DELTA_FILTER) continue;
                            double delta = asyncTable.getDelta(k);
                            double value = asyncTable.getValue(k);
                            double f = value - Math.max(value, delta);
                            if (f >= threshold) {
                                if (asyncConfig.isSync()) {
                                    if (asyncTable.updateLockFree(k, checkThread.iter)) updateCounter.addAndGet(1);
                                } else {
                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                                }
                            }
                        }
                    }
                    if (barrier != null) barrier.await();
                }
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        private void arrangeTask() {
            int threadNum = AsyncConfig.get().getThreadNum();
            int size = asyncTable.getSize();
            int blockSize = size / threadNum;
            if (blockSize == 0) {
//                L.warn("too many threads asynctable size " + size);
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
        protected int iter = 0;

        protected CheckThread() {
        }

        @Override
        public void run() {
            iter++;
            if (stopWatch == null) {
                stopWatch = new StopWatch();
                stopWatch.start();
            }
        }

        protected void done() {
            stop = true;
            stopWatch.stop();
            L.info("UPDATE_TIMES:" + updateCounter.get());
            L.info("DONE ELAPSED:" + stopWatch.getTime());
            L.info("CHECKER THREAD EXIT");
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