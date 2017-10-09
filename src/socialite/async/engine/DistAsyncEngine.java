package socialite.async.engine;

import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.codegen.AsyncCodeGenMain;
import socialite.async.dist.MsgType;
import socialite.async.dist.Payload;
import socialite.async.util.SerializeTool;
import socialite.codegen.Analysis;
import socialite.engine.ClientEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistAsyncEngine implements Runnable {
    private static final Log L = LogFactory.getLog(DistAsyncEngine.class);
    private int workerNum;
    private StopWatch stopWatch;
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
        @Override
        public void run() {
            try {
                while (true) {
                    double accumulateSum =
                            IntStream.rangeClosed(1, workerNum).parallel().mapToDouble(dest -> {
                                double[] recvBuf = new double[1];
                                MPI.COMM_WORLD.Sendrecv(new byte[1], 0, 1, MPI.BYTE, dest, MsgType.REQUIRE_TERM_CHECK.ordinal(),
                                        recvBuf, 0, 1, MPI.DOUBLE, dest, MsgType.TERM_CHECK_PARTIAL_VALUE.ordinal());//send term check request and receive partial value
                                return recvBuf[0];
                            }).reduce(Double::sum).orElse(-1);
                    if (stopWatch == null) {
                        stopWatch = new StopWatch();
                        stopWatch.start();
                    }
                    L.info("TERM_CHECK_VALUE_SUM: " + accumulateSum);
                    boolean[] termOrNot = new boolean[]{eval(accumulateSum)};
                    IntStream.rangeClosed(1, workerNum).parallel().forEach(dest -> MPI.COMM_WORLD.Send(termOrNot, 0, 1, MPI.BOOLEAN, dest, MsgType.TERM_CHECK_FEEDBACK.ordinal()));
                    if (termOrNot[0]) {
                        stopWatch.stop();
                        L.info("TERM_CHECK_DETERMINED_TO_STOP ELAPSED " + stopWatch.getTime());
                        break;
                    }
                    Thread.sleep(AsyncConfig.get().getCheckInterval());
                }
            } catch (MPIException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        private boolean eval(double val) {
            double threshold = AsyncConfig.get().getThreshold();
            switch (AsyncConfig.get().getCond()) {
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
