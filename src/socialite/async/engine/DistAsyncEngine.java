package socialite.async.engine;

import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.codegen.AsyncCodeGenMain;
import socialite.async.codegen.BaseAsyncRuntime;
import socialite.async.dist.MsgType;
import socialite.async.dist.Payload;
import socialite.async.util.SerializeTool;
import socialite.codegen.Analysis;
import socialite.engine.ClientEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;
import socialite.util.SociaLiteException;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistAsyncEngine implements Runnable {
    private static final Log L = LogFactory.getLog(DistAsyncEngine.class);
    private int workerNum;
    private ClientEngine clientEngine;
    private AsyncAnalysis asyncAnalysis;
    private AsyncCodeGenMain asyncCodeGenMain;

    public DistAsyncEngine(String program) {
        workerNum = MPI.COMM_WORLD.Size() - 1;
        clientEngine = new ClientEngine();
        Parser parser = new Parser(program);
        parser.parse(program);
        Analysis tmpAn = new Analysis(parser);
        tmpAn.run();

        asyncAnalysis = new AsyncAnalysis(tmpAn);
        List<String> decls = parser.getTableDeclMap().values().stream().map(TableDecl::getDeclText).collect(Collectors.toList());
        List<Rule> rules = tmpAn.getEpochs().stream().flatMap(epoch -> epoch.getRules().stream()).filter(rule -> !(rule instanceof DeltaRule)
                && !rule.toString().contains("Remote_")).collect(Collectors.toList()); //get rid of DeltaRule and Remote_rule
        //由socialite执行表创建和非递归规则
        if (!AsyncConfig.get().isDebugging())
            decls.forEach(stat -> {
                L.info(stat);
                clientEngine.run(stat);
            });

        boolean existLeftRec = rules.stream().anyMatch(Rule::isLeftRec);
        for (Rule rule : rules) {
            boolean added = false;
            if (existLeftRec) {
                if (rule.isLeftRec()) {
                    asyncAnalysis.addRecRule(rule);
                    added = true;
                }
            } else if (rule.inScc()) {
                asyncAnalysis.addRecRule(rule);
                added = true;
            }
            if (!AsyncConfig.get().isDebugging())
                if (!added) {
                    L.info("exec rule " + rule.getRuleText());
                    clientEngine.run(rule.getRuleText());
                }
        }
    }

    private void compile() {
        if (asyncAnalysis.analysis()) {
            asyncCodeGenMain = new AsyncCodeGenMain(asyncAnalysis);
            asyncCodeGenMain.generateDist();
            if (AsyncConfig.get().isPriority()) {
                if (asyncAnalysis.getAggrName().equals("dcount") || asyncAnalysis.getAggrName().equals("dsum"))
                    AsyncConfig.get().setPriorityType(AsyncConfig.PriorityType.SUM_COUNT);
                else if (asyncAnalysis.getAggrName().equals("dmin"))
                    AsyncConfig.get().setPriorityType(AsyncConfig.PriorityType.MIN);
                else if (asyncAnalysis.getAggrName().equals("dmax"))
                    AsyncConfig.get().setPriorityType(AsyncConfig.PriorityType.MAX);
                else throw new SociaLiteException("unsupported priority");
            }
        }
    }

    @Override
    public void run() {
        compile();
        sendCmd();
        FeedBackThread feedBackThread = new FeedBackThread();
        feedBackThread.start();
        try {
            feedBackThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        clientEngine.shutdown();
    }

    private void sendCmd() {
        List<String> initStats = asyncCodeGenMain.getInitStats();
        if (!AsyncConfig.get().isDebugging())
            initStats.forEach(initStat -> clientEngine.run(initStat));
        Map<Integer, Integer> myIdxWorkerIdMap = new HashMap<>();
        IntStream.rangeClosed(1, workerNum).forEach(source -> {
            int[] buff = new int[2];
            MPI.COMM_WORLD.Recv(buff, 0, 2, MPI.INT, source, MsgType.REPORT_MYIDX.ordinal());
            myIdxWorkerIdMap.put(buff[0], buff[1]);
        });

        LinkedHashMap<String, byte[]> compiledClasses = asyncCodeGenMain.getCompiledClasses();
        Payload payload = new Payload(AsyncConfig.get(), myIdxWorkerIdMap, compiledClasses, asyncAnalysis.getEdgePName());

        SerializeTool serializeTool = new SerializeTool.Builder().build();
        byte[] data = serializeTool.toBytes(payload);
        IntStream.rangeClosed(1, workerNum).forEach(dest ->
                MPI.COMM_WORLD.Send(data, 0, data.length, MPI.BYTE, dest, MsgType.NOTIFY_INIT.ordinal())
        );
    }

    private class FeedBackThread extends Thread {
        private AsyncConfig asyncConfig;
        private StopWatch stopWatch;
        private Double lastSum;
        double accumulatedSum = 0;
        double totalUpdateTimes = 0;
        private int iter;

        private FeedBackThread() {
            asyncConfig = AsyncConfig.get();
        }

        @Override
        public void run() {

            double[] partialValue = new double[4];
            boolean[] termOrNot = new boolean[1];

            try {
                while (true) {
                    accumulatedSum = 0;
                    double totalRx = 0, totalTx = 0;
                    if (asyncConfig.isSync() || asyncConfig.isBarrier()) {
                        for (int source = 1; source <= workerNum; source++) {
                            MPI.COMM_WORLD.Recv(partialValue, 0, 4, MPI.DOUBLE, source, MsgType.REQUIRE_TERM_CHECK.ordinal());
                            accumulatedSum += partialValue[0];
                            totalUpdateTimes += partialValue[1];
                            totalRx += partialValue[2];
                            totalTx += partialValue[3];
                        }
                        //when first received feedback, we start stopwatch
                        if (stopWatch == null) {
                            stopWatch = new StopWatch();
                            stopWatch.start();
                        }
                        termOrNot[0] = isTerm();
                        if (asyncConfig.isNetworkInfo())
                            L.info(String.format("RX: %f MB TX: %f MB", totalRx / 1024 / 1024, totalTx / 1024 / 1024));
                        IntStream.rangeClosed(1, workerNum).forEach(dest ->
                                MPI.COMM_WORLD.Send(termOrNot, 0, 1, MPI.BOOLEAN, dest, MsgType.TERM_CHECK_FEEDBACK.ordinal()));
                        if (termOrNot[0]) {
                            stopWatch.stop();
                            L.info("SYNC/BARRIER MODE - TERM_CHECK_DETERMINED_TO_STOP ELAPSED " + stopWatch.getTime());
                            break;
                        }
                    } else {
                        //sleep first to prevent stop before compute
                        Thread.sleep(AsyncConfig.get().getCheckInterval());
                        for (int dest = 1; dest <= workerNum; dest++) {
                            MPI.COMM_WORLD.Sendrecv(new byte[1], 0, 1, MPI.BYTE, dest, MsgType.REQUIRE_TERM_CHECK.ordinal(),
                                    partialValue, 0, 4, MPI.DOUBLE, dest, MsgType.TERM_CHECK_PARTIAL_VALUE.ordinal());//send term check request and receive partial value
                            accumulatedSum += partialValue[0];
                            totalUpdateTimes += partialValue[1];
                            totalRx += partialValue[2];
                            totalTx += partialValue[3];
                        }
                        //when first received partial value, we start stopwatch
                        if (stopWatch == null) {
                            stopWatch = new StopWatch();
                            stopWatch.start();
                        }

                        termOrNot[0] = isTerm();
                        if (asyncConfig.isNetworkInfo())
                            L.info(String.format("RX: %f MB TX: %f MB", totalRx / 1024 / 1024, totalTx / 1024 / 1024));

                        IntStream.rangeClosed(1, workerNum).parallel().forEach(dest -> MPI.COMM_WORLD.Send(termOrNot, 0, 1, MPI.BOOLEAN, dest, MsgType.TERM_CHECK_FEEDBACK.ordinal()));
                        if (termOrNot[0]) {
                            stopWatch.stop();
                            L.info("ASYNC MODE - TERM_CHECK_DETERMINED_TO_STOP ELAPSED " + stopWatch.getTime());
                            break;
                        }
                    }
                }

            } catch (MPIException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        private boolean isTerm() {
            L.info("TOTAL UPDATE TIMES " + totalUpdateTimes);
            if (asyncConfig.isSync() || asyncConfig.isBarrier()) {
                L.info("ITER: " + ++iter);
            }
            if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE)
                L.info("TERM_CHECK_VALUE_SUM: " + new BigDecimal(accumulatedSum));
            else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA)
                L.info("TERM_CHECK_DELTA_SUM: " + new BigDecimal(accumulatedSum));
            else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_VALUE) {
                if (lastSum == null) {
                    lastSum = accumulatedSum;
                    return false;
                } else {
                    double tmp = accumulatedSum;
                    accumulatedSum = Math.abs(lastSum - accumulatedSum);
                    lastSum = tmp;
                }
                L.info("TERM_CHECK_DIFF_VALUE_SUM: " + new BigDecimal(accumulatedSum));
            } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_DELTA) {
                if (lastSum == null) {
                    lastSum = accumulatedSum;
                    return false;
                } else {
                    double tmp = accumulatedSum;
                    accumulatedSum = Math.abs(lastSum - accumulatedSum);
                    lastSum = tmp;
                }
                L.info("TERM_CHECK_DIFF_DELTA_SUM: " + new BigDecimal(accumulatedSum));
            }
            return BaseAsyncRuntime.eval(accumulatedSum);
        }
    }
}
