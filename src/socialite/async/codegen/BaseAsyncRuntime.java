package socialite.async.codegen;

//AsyncRuntime(initSize, threadNum, dynamic, checkType, checkerInterval, threshold, cond) ::= <<

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.util.ResettableCountDownLatch;
import socialite.tables.TableInst;

import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public abstract class BaseAsyncRuntime implements Runnable {
    protected static final Log L = LogFactory.getLog(BaseAsyncRuntime.class);
    protected AsyncConfig asyncConfig;
    private volatile boolean stop;
    protected ComputingThread[] computingThreads;
    protected SchedulerThread schedulerThread;
    protected CheckThread checkerThread;
    protected BaseAsyncTable asyncTable;
    protected CyclicBarrier barrier;
    protected AtomicInteger updateCounter;
    protected ResettableCountDownLatch countDownLatch;
    private volatile double priorityThreshold;

    private volatile boolean check;
    private final Object lock = new Object();
    private long lastCheckTime;

    protected abstract boolean loadData(TableInst[] initTableInstArr, TableInst[] edgeTableInstArr);

    protected void createThreads() {
        asyncConfig = AsyncConfig.get();
        updateCounter = new AtomicInteger();
        int half = asyncConfig.getThreadNum() / 2;
        if (asyncConfig.isPriority()) countDownLatch = new ResettableCountDownLatch(half == 0 ? 1 : half);
        priorityThreshold = -Double.MAX_VALUE;

        int threadNum = asyncConfig.getThreadNum();
        computingThreads = new ComputingThread[threadNum];
        IntStream.range(0, threadNum).forEach(i -> computingThreads[i] = new ComputingThread(i));
        if (AsyncConfig.get().isPriority()) schedulerThread = new SchedulerThread();
    }

    public BaseAsyncTable getAsyncTable() {
        return asyncTable;
    }

    protected class ComputingThread extends Thread {
        private volatile int start;
        private volatile int end;
        private int tid;
        double[] deltaSample;
        final double SCHEDULE_PORTION;
        boolean assigned;
        private AsyncConfig asyncConfig;
        private ThreadLocalRandom randomGenerator;

        public ComputingThread(int tid) {
            this.tid = tid;
            asyncConfig = AsyncConfig.get();
            randomGenerator = ThreadLocalRandom.current();
            SCHEDULE_PORTION = asyncConfig.getSchedulePortion();
            deltaSample = new double[1000];
        }


        @Override
        public void run() {
            if (tid == 0) lastCheckTime = System.currentTimeMillis();
            if (!asyncConfig.isPriority() || asyncConfig.isPriorityLocal()) {
                if (asyncConfig.isPriority())
                    L.info("PRIORITY LOCAL!!!!!!!!!!!!!!");
                try {
                    while (!stop) {
//                        if (!assigned || asyncConfig.isDynamic()) {
//                            arrangeTask();
//                            assigned = true;
//                        }

                        //empty thread, sleep to reduce CPU race
                        if (start == end) {
                            Thread.sleep(10);
                            if (barrier != null) barrier.await();
                            continue;
                        }
                        double threshold;
                        if(!asyncConfig.isSync() && ! asyncConfig.isBarrier()) {
                            for (int i = 0; i < deltaSample.length; i++) {
                                int ind = randomGenerator.nextInt(start, end);
                                deltaSample[i] = asyncTable.getPriority(ind);
                            }

//                            boolean update = false;
//                            for (double delta : deltaSample)
//                                if (delta != 0) {
//                                    update = true;
//                                    break;
//                                }
//                            if (!update) {
////                                L.info("got it");
//                                Thread.sleep(1);
//                                continue;
//                            }
                        }
                        if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.NONE) {
                            for (int k = start; k < end; k++) {
                                if (asyncConfig.isSync()) {
                                    if (asyncTable.updateLockFree(k, checkerThread.iter)) updateCounter.addAndGet(1);
                                } else {
                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                                }
                            }
                        } else {
                            Arrays.sort(deltaSample);
                            int cutIndex = (int) (deltaSample.length * (1 - SCHEDULE_PORTION));
                            if (cutIndex == 0)
                                threshold = -Double.MAX_VALUE;
                            else
                                threshold = deltaSample[cutIndex];
                            for (int k = start; k < end; k++) {
                                double delta = asyncTable.getPriority(k);
                                if (delta >= threshold) {
                                    if (asyncConfig.isSync()) {
                                        if (asyncTable.updateLockFree(k, checkerThread.iter))
                                            updateCounter.addAndGet(1);
                                    } else {
                                        if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                                    }
                                }
                            }
                        }
                        if (barrier != null) barrier.await();
                        else {
                            if (tid == 0) {
                                if (System.currentTimeMillis() - lastCheckTime >= checkerThread.CHECKER_INTERVAL) {
                                    checkerThread.notifyCheck();
                                    lastCheckTime = System.currentTimeMillis();
                                }
                            }
                        }
                    }
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            } else {
                L.info("PRIORITY GLOBALLLLLLLLLLL");
                try {
                    while (!stop) {
//                        if (!assigned || asyncConfig.isDynamic()) {
//                            arrangeTask();
//                            assigned = true;
//                        }

                        //empty thread, sleep to reduce CPU race
                        if (start == end) {
                            Thread.sleep(10);
                            if (barrier != null) barrier.await();
                            if (countDownLatch != null) countDownLatch.countDown();
                            continue;
                        }

                        if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.NONE) {
                            for (int k = start; k < end; k++) {
                                if (asyncConfig.isSync()) {
                                    if (asyncTable.updateLockFree(k, checkerThread.iter)) updateCounter.addAndGet(1);
                                } else {
                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                                }
                            }
                        } else {
                            for (int k = start; k < end; k++) {
                                double delta = asyncTable.getPriority(k);
                                if (delta >= priorityThreshold) {
                                    if (asyncConfig.isSync()) {
                                        if (asyncTable.updateLockFree(k, checkerThread.iter))
                                            updateCounter.addAndGet(1);
                                    } else {
                                        if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
                                    }
                                }
                            }
                        }
                        if (countDownLatch != null) countDownLatch.countDown();
                        if (barrier != null) barrier.await();
                        else {
                            if (tid == 0) {
                                if (System.currentTimeMillis() - lastCheckTime >= checkerThread.CHECKER_INTERVAL) {
                                    checkerThread.notifyCheck();
                                    lastCheckTime = System.currentTimeMillis();
                                }
                            }
                        }
                    }
                    if (countDownLatch != null) countDownLatch.countDown();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        }


        @Override
        public String toString() {
            return String.format("id: %d range: [%d, %d)", tid, start, end);
        }
    }

    protected void arrangeTask() {
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
        }
    }

//    protected class ComputingThread extends Thread {
//        private volatile int start;
//        private volatile int end;
//        private int tid;
//        boolean assigned;
//        private AsyncConfig asyncConfig;
//
//        public ComputingThread(int tid) {
//            this.tid = tid;
//            asyncConfig = AsyncConfig.get();
//        }
//
//
//        @Override
//        public void run() {
//            try {
//                while (!stop) {
//                    if (!assigned || asyncConfig.isDynamic()) {
//                        arrangeTask();
//                        assigned = true;
//                    }
//
//                    //empty thread, sleep to reduce CPU race
//                    if (start == end) {
//                        Thread.sleep(10);
//                        if (barrier != null) barrier.await();
//                        if (countDownLatch != null) countDownLatch.countDown();
//                        continue;
//                    }
//
//                    if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.NONE) {
//                        for (int k = start; k < end; k++) {
//                            if (asyncConfig.isSync()) {
//                                if (asyncTable.updateLockFree(k, checkerThread.iter)) updateCounter.addAndGet(1);
//                            } else {
//                                if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
//                            }
//                        }
//                    } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.SUM_COUNT) {
//                        for (int k = start; k < end; k++) {
//                            double delta = asyncTable.getDelta(k);
//                            if (delta >= priorityThreshold) {
//                                if (asyncConfig.isSync()) {
//                                    if (asyncTable.updateLockFree(k, checkerThread.iter)) updateCounter.addAndGet(1);
//                                } else {
//                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
//                                }
//                            }
//                        }
//                    } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.MIN) {
//                        for (int k = start; k < end; k++) {
//                            double delta = asyncTable.getDelta(k);
//                            double value = asyncTable.getValue(k);
//                            double f = value - Math.min(value, delta);
//                            if (f >= priorityThreshold) {
//                                if (asyncConfig.isSync()) {
//                                    if (asyncTable.updateLockFree(k, checkerThread.iter)) updateCounter.addAndGet(1);
//                                } else {
//                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
//                                }
//                            }
//                        }
//                    } else if (asyncConfig.getPriorityType() == AsyncConfig.PriorityType.MAX) {
//                        for (int k = start; k < end; k++) {
//                            double delta = asyncTable.getDelta(k);
//                            double value = asyncTable.getValue(k);
//                            double f = value - Math.max(value, delta);
//                            if (f >= priorityThreshold) {
//                                if (asyncConfig.isSync()) {
//                                    if (asyncTable.updateLockFree(k, checkerThread.iter)) updateCounter.addAndGet(1);
//                                } else {
//                                    if (asyncTable.updateLockFree(k)) updateCounter.addAndGet(1);
//                                }
//                            }
//                        }
//                    }
//                    if (barrier != null) barrier.await();
//                    if (countDownLatch != null) countDownLatch.countDown();
//                }
//            } catch (InterruptedException | BrokenBarrierException e) {
//                e.printStackTrace();
//            }
//        }
//
//        private void arrangeTask() {
//            int threadNum = AsyncConfig.get().getThreadNum();
//            int size = asyncTable.getSize();
//            int blockSize = size / threadNum;
//            if (blockSize == 0) {
////                L.warn("too many threads asynctable size " + size);
//                blockSize = size;
//            }
//
//            for (int tid = 0; tid < threadNum; tid++) {
//                int start = tid * blockSize;
//                int end = (tid + 1) * blockSize;
//                if (tid == threadNum - 1)//last thread, assign all
//                    end = size;
//                if (start >= size) {//assign empty tasks
//                    start = 0;
//                    end = 0;
//                } else if (end > size || tid == threadNum - 1) {//block < lastThread's tasks or block > ~
//                    end = size;
//                }
//                computingThreads[tid].start = start;
//                computingThreads[tid].end = end;
//            }
//        }
//
//
//        @Override
//        public String toString() {
//            return String.format("id: %d range: [%d, %d)", tid, start, end);
//        }
//    }

    protected class SchedulerThread extends Thread {
        AsyncConfig asyncConfig;
        private ThreadLocalRandom randomGenerator;
        double[] deltaSample;
        final double SCHEDULE_PORTION;

        SchedulerThread() {
            asyncConfig = AsyncConfig.get();
            randomGenerator = ThreadLocalRandom.current();
            SCHEDULE_PORTION = asyncConfig.getSchedulePortion();
            deltaSample = new double[1000];
        }

        @Override
        public void run() {

            while (!stop) {
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                int size = asyncTable.getSize();


                for (int i = 0; i < deltaSample.length; i++) {
                    int ind = randomGenerator.nextInt(0, size);
                    deltaSample[i] = asyncTable.getPriority(ind);
                }


//                boolean update = false;
//                for (double delta : deltaSample)
//                    if (delta != 0) {
//                        update = true;
//                        break;
//                    }
//                if (!update) {
//                    L.info("got it");
//                    priorityThreshold = Double.MAX_VALUE;
//                    continue;
//                }

                if (deltaSample.length == 0) {//no sample, schedule all
                    priorityThreshold = -Double.MAX_VALUE;
                } else {
                    Arrays.sort(deltaSample);
                    int cutIndex = (int) (deltaSample.length * (1 - SCHEDULE_PORTION));
                    if (cutIndex == 0)
                        priorityThreshold = -Double.MAX_VALUE;
                    else
                        priorityThreshold = deltaSample[cutIndex];
                }
                countDownLatch.reset();
            }
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
            L.info("call done");
            stop = true;
            stopWatch.stop();
            L.info("UPDATE_TIMES:" + updateCounter.get());
            L.info("DONE ELAPSED:" + stopWatch.getTime());
            L.info("CHECKER THREAD EXITED");
        }

        void waitingCheck() throws InterruptedException {
            synchronized (lock) {
                while (!check) {
                    lock.wait();
                }
            }
            check = false;
        }

        void notifyCheck() {
            synchronized (lock) {
                check = true;
                lock.notify();
            }
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