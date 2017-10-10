package socialite.async.codegen;

//AsyncRuntime(initSize, threadNum, dynamic, checkType, checkerInterval, threshold, cond) ::= <<

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.tables.TableInst;
import socialite.visitors.VisitorImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

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
        return true;
    }

    @Override
    protected void createThreads() {
        int threadNum = AsyncConfig.get().getThreadNum();
        computingThreads = new ComputingThread[threadNum];
        checkerThread = new CheckThread();
        checkerThread.setPriority(Thread.MAX_PRIORITY);
    }


    @Override
    public void run() {
        loadData(initTableInstArr, edgeTableInstArr);
        createThreads();
        arrangeTask();
        L.info("Data Loaded size:" + asyncTable.getSize());
        checkerThread.start();
        Arrays.stream(computingThreads).forEach(ComputingThread::start);
        L.info("Worker started");
        try {
            for (ComputingThread worker : computingThreads)
                worker.join();
            checkerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    protected class CheckThread extends BaseAsyncRuntime.CheckThread {
        private AsyncConfig asyncConfig;

        CheckThread() {
            asyncConfig = AsyncConfig.get();
        }

        @Override
        public void run() {
            stopWatch.start();
            while (true) {
                try {
                    checkerSW.reset();
                    checkerSW.start();
                    double sum = 0.0d;
                    boolean valid = true;
                    if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA) {
                        if (asyncTable.accumulateDelta() instanceof Integer) {
                            Integer deltaSum = (Integer) asyncTable.accumulateDelta();
                            if (deltaSum == Integer.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        } else if (asyncTable.accumulateDelta() instanceof Long) {
                            Long deltaSum = (Long) asyncTable.accumulateDelta();
                            if (deltaSum == Long.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        } else if (asyncTable.accumulateDelta() instanceof Float) {
                            Float deltaSum = (Float) asyncTable.accumulateDelta();
                            if (deltaSum == Long.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        } else if (asyncTable.accumulateDelta() instanceof Double) {
                            Double deltaSum = (Double) asyncTable.accumulateDelta();
                            if (deltaSum == Double.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        }
                        L.info("sum of value: " + sum);
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE) {
                        if (asyncTable.accumulateValue() instanceof Integer) {
                            Integer deltaSum = (Integer) asyncTable.accumulateValue();
                            if (deltaSum == Integer.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        } else if (asyncTable.accumulateValue() instanceof Long) {
                            Long deltaSum = (Long) asyncTable.accumulateValue();
                            if (deltaSum == Long.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        } else if (asyncTable.accumulateValue() instanceof Float) {
                            Float deltaSum = (Float) asyncTable.accumulateValue();
                            if (deltaSum == Long.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        } else if (asyncTable.accumulateValue() instanceof Double) {
                            Double deltaSum = (Double) asyncTable.accumulateDelta();
                            if (deltaSum == Double.MAX_VALUE) valid = false;
                            sum = deltaSum;
                        }
                        L.info("sum of delta: " + sum);
                    }
                    if (valid && eval(sum)) {
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
    }
}