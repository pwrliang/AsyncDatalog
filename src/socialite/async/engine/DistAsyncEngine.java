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
import socialite.async.util.TextUtils;
import socialite.codegen.Analysis;
import socialite.engine.ClientEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistAsyncEngine implements Runnable {
    public static final double THRESHOLD = 0.9999;
    public static final int TERM_CHECK_INTERVAL = 2000;
    private static final Log L = LogFactory.getLog(DistAsyncEngine.class);
    private int workerNum;
    private StopWatch stopWatch;
    private ClientEngine clientEngine;
    private AsyncAnalysis asyncAnalysis;
    private AsyncCodeGenMain asyncCodeGenMain;

    public DistAsyncEngine(String program) {
        workerNum = MPI.COMM_WORLD.Rank() - 1;
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
            decls.forEach(clientEngine::run);

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
                if (!added)
                    clientEngine.run(rule.getRuleText());
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
        loadData();
        FeedBackThread feedBackThread = new FeedBackThread();
        feedBackThread.start();
        try {
            feedBackThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void loadData() {
        List<String> initStats = asyncCodeGenMain.getInitStats();
        if (!AsyncConfig.get().isDebugging())
            initStats.forEach(initStat -> clientEngine.run(initStat));
        LinkedHashMap<String, byte[]> compiledClasses = asyncCodeGenMain.getCompiledClasses();
        Payload payload = new Payload(compiledClasses, asyncAnalysis.getEdgePName());

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
                    Thread.sleep(TERM_CHECK_INTERVAL);
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
                    boolean[] termOrNot = new boolean[]{accumulateSum > THRESHOLD};
                    IntStream.rangeClosed(1, workerNum).parallel().forEach(dest -> MPI.COMM_WORLD.Send(termOrNot, 0, 1, MPI.BOOLEAN, dest, MsgType.TERM_CHECK_FEEDBACK.ordinal()));
                    if (termOrNot[0]) {
                        stopWatch.stop();
                        L.info("TERM_CHECK_DETERMINED_TO_STOP ELAPSED " + stopWatch.getTime());
                        break;
                    }
                }
            } catch (MPIException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
