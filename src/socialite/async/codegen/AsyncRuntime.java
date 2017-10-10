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
                    double sum = 0.0d;
                    if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA) {
                        if (asyncTable.accumulateDelta() instanceof Integer) {
                            sum = (Integer) asyncTable.accumulateDelta();
                        } else if (asyncTable.accumulateDelta() instanceof Long) {
                            sum = (Long) asyncTable.accumulateDelta();
                        } else if (asyncTable.accumulateDelta() instanceof Float) {
                            sum = (Float) asyncTable.accumulateDelta();
                        } else if (asyncTable.accumulateDelta() instanceof Double) {
                            sum = (Double) asyncTable.accumulateDelta();
                        }
                        L.info("sum of delta: " + new BigDecimal(sum));
                    } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE) {
                        if (asyncTable.accumulateValue() instanceof Integer) {
                            sum = (Integer) asyncTable.accumulateValue();
                        } else if (asyncTable.accumulateValue() instanceof Long) {
                            sum = (Long) asyncTable.accumulateValue();
                        } else if (asyncTable.accumulateValue() instanceof Float) {
                            sum = (Float) asyncTable.accumulateValue();
                        } else if (asyncTable.accumulateValue() instanceof Double) {
                            sum = (Double) asyncTable.accumulateDelta();
                        }
                        L.info("sum of value: " + new BigDecimal(sum));
                    }
                    if (eval(sum)) {
                        done();
                        break;
                    }
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