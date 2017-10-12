package socialite.async.codegen;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.tables.TableInst;
import socialite.visitors.VisitorImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;

public class AsyncRuntime extends BaseAsyncRuntime {
    private static final Log L = LogFactory.getLog(AsyncRuntime.class);
    private CheckThread checkerThread;
    private TableInst[] initTableInstArr;
    private TableInst[] edgeTableInstArr;

    public AsyncRuntime(BaseAsyncTable asyncTable, TableInst[] initTableInstArr, TableInst[] edgeTableInstArr) {
        super.asyncTable = asyncTable;
        this.initTableInstArr = initTableInstArr;
        this.edgeTableInstArr = edgeTableInstArr;
    }

    @Override
    protected boolean loadData(TableInst[] initTableInstArr, TableInst[] edgeTableInstArr) {
        try {
            Method method;
            if (AsyncConfig.get().isDynamic()) {
                //动态算法需要edge做连接，如prog4、9!>
                method = edgeTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
                for (TableInst tableInst : edgeTableInstArr) {
                    if (!tableInst.isEmpty()) {
                        method.invoke(tableInst, asyncTable.getEdgeVisitor());
                        tableInst.clear();
                    }
                }
            }
            method = initTableInstArr[0].getClass().getDeclaredMethod("iterate", VisitorImpl.class);
            for (TableInst tableInst : initTableInstArr) {
                if (!tableInst.isEmpty()) {
                    method.invoke(tableInst, asyncTable.getInitVisitor());
                    tableInst.clear();
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    protected void createThreads() {
        int threadNum = AsyncConfig.get().getThreadNum();
        computingThreads = new ComputingThread[threadNum];
        IntStream.range(0, threadNum).forEach(i -> computingThreads[i] = new ComputingThread(i));
        checkerThread = new CheckThread();
        checkerThread.setPriority(Thread.MAX_PRIORITY);
        if (AsyncConfig.get().isSync()) barrier = new CyclicBarrier(threadNum, checkerThread);
    }


    @Override
    public void run() {
        L.info("RECV CMD NOTIFY_INIT CONFIG:" + AsyncConfig.get());
        loadData(initTableInstArr, edgeTableInstArr);
        createThreads();
        L.info("Data Loaded size:" + asyncTable.getSize());
        if (!AsyncConfig.get().isSync())
            checkerThread.start();
        Arrays.stream(computingThreads).forEach(ComputingThread::start);
        L.info("Worker started");
        try {
            for (ComputingThread worker : computingThreads)
                worker.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    protected class CheckThread extends BaseAsyncRuntime.CheckThread {
        private AsyncConfig asyncConfig;
        private Double lastSum = null;

        CheckThread() {
            asyncConfig = AsyncConfig.get();
        }

        @Override
        public void run() {
            super.run();
            while (true) {
                try {
                    double sum = 0.0d;
                    boolean skipFirst = false;
                    Object accumulated;
                    if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE) {
                        accumulated = asyncTable.accumulateValue();
                        if (accumulated instanceof Integer) {
                            sum = (Integer) accumulated;
                        } else if (accumulated instanceof Long) {
                            sum = (Long) accumulated;
                        } else if (accumulated instanceof Float) {
                            sum = (Float) accumulated;
                        } else if (accumulated instanceof Double) {
                            sum = (Double) accumulated;
                        }
                        L.info("sum of value: " + new BigDecimal(sum));
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA) {
                        accumulated = asyncTable.accumulateDelta();
                        if (accumulated instanceof Integer) {
                            sum = (Integer) accumulated;
                        } else if (accumulated instanceof Long) {
                            sum = (Long) accumulated;
                        } else if (accumulated instanceof Float) {
                            sum = (Float) accumulated;
                        } else if (accumulated instanceof Double) {
                            sum = (Double) accumulated;
                        }
                        L.info("sum of delta: " + new BigDecimal(sum));
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_VALUE) {
                        accumulated = asyncTable.accumulateValue();
                        if (accumulated instanceof Integer) {
                            sum = (Integer) accumulated;
                        } else if (accumulated instanceof Long) {
                            sum = (Long) accumulated;
                        } else if (accumulated instanceof Float) {
                            sum = (Float) accumulated;
                        } else if (accumulated instanceof Double) {
                            sum = (Double) accumulated;
                        }
                        if (lastSum == null) {
                            lastSum = sum;
                            skipFirst = true;
                        } else {
                            double tmp = sum;
                            sum = Math.abs(lastSum - sum);
                            lastSum = tmp;
                        }
                        L.info("diff sum of value: " + new BigDecimal(sum));
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_DELTA) {
                        accumulated = asyncTable.accumulateDelta();
                        if (accumulated instanceof Integer) {
                            sum = (Integer) accumulated;
                        } else if (accumulated instanceof Long) {
                            sum = (Long) accumulated;
                        } else if (accumulated instanceof Float) {
                            sum = (Float) accumulated;
                        } else if (accumulated instanceof Double) {
                            sum = (Double) accumulated;
                        }
                        if (lastSum == null) {
                            lastSum = sum;
                            skipFirst = true;
                        } else {
                            double tmp = sum;
                            sum = Math.abs(lastSum - sum);
                            lastSum = tmp;
                        }
                        L.info("diff sum of delta: " + new BigDecimal(sum));
                    }

                    if (!skipFirst && eval(sum)) {
                        done();
                        break;
                    }
                    if (barrier != null)
                        break;
                    Thread.sleep(CHECKER_INTERVAL);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}