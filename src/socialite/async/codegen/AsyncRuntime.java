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

public class AsyncRuntime extends BaseAsyncRuntime {
    private static final Log L = LogFactory.getLog(AsyncRuntime.class);
    //    private BaseAsyncRuntime.CheckThread checkerThread;
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
    public void run() {
        L.info("RECV CMD NOTIFY_INIT CONFIG:" + AsyncConfig.get());
        loadData(initTableInstArr, edgeTableInstArr);
        super.createThreads();
        arrangeTask();
        checkerThread = new AsyncRuntime.CheckThread();
        if (AsyncConfig.get().isSync() || AsyncConfig.get().isBarrier())
            barrier = new CyclicBarrier(asyncConfig.getThreadNum(), checkerThread);

        L.info("Data Loaded size:" + asyncTable.getSize());
        Arrays.stream(computingThreads).forEach(ComputingThread::start);
        if (AsyncConfig.get().isPriority() && !AsyncConfig.get().isPriorityLocal()) schedulerThread.start();
        if (!AsyncConfig.get().isSync() && !AsyncConfig.get().isBarrier())
            checkerThread.start();
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
//                    if (barrier == null)
//                        waitingCheck();
                    Thread.sleep(asyncConfig.getCheckInterval());
                    if(asyncConfig.isDynamic())
                        arrangeTask();
                    double sum = 0.0d;
                    boolean skipFirst = false;
                    if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE) {
                        sum = asyncTable.accumulateValue();
                        L.info("sum of value: " + new BigDecimal(sum));
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA) {
                        sum = asyncTable.accumulateDelta();
                        L.info("sum of delta: " + new BigDecimal(sum));
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_VALUE) {
                        sum = asyncTable.accumulateValue();
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
                        sum = asyncTable.accumulateDelta();
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
                    L.info("UPDATE TIMES:" + updateCounter.get());
                    if (asyncConfig.isSync() || asyncConfig.isBarrier())
                        L.info("ITER: " + iter);
                    if (!skipFirst && eval(sum)) {
                        done();
                        break;
                    }
                    if (barrier != null)//sync mode
                        break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}