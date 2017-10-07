package socialite.async.engine;

import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.codegen.DistAsyncCodeGen;
import socialite.async.dist.ds.MsgType;
import socialite.async.util.SerializeTool;
import socialite.async.util.TextUtils;
import socialite.codegen.Analysis;
import socialite.engine.ClientEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistAsyncEngine implements Runnable {
    public static final double THRESHOLD = 0.9999;
    public static final int TERM_CHECK_INTERVAL = 2000;
    private static final Log L = LogFactory.getLog(DistAsyncEngine.class);
    private int workerNum;
    private StopWatch stopWatch;
        private ClientEngine clientEngine=new ClientEngine();
//    private LocalEngine clientEngine=new LocalEngine();
    private AsyncAnalysis asyncAnalysis;
    private DistAsyncCodeGen distAsyncCodeGen;


    public DistAsyncEngine(String program, int workerNum) {
        this.workerNum = workerNum;

//        clientEngine = new ClientEngine();
        Parser parser = new Parser(program);
        parser.parse(program);
        Analysis tmpAn = new Analysis(parser);
        tmpAn.run();

        asyncAnalysis = new AsyncAnalysis(tmpAn);
        List<String> decls = parser.getTableDeclMap().values().stream().map(TableDecl::getDeclText).collect(Collectors.toList());
        List<Rule> rules = tmpAn.getEpochs().stream().flatMap(epoch -> epoch.getRules().stream()).filter(rule -> !(rule instanceof DeltaRule)
                && !rule.toString().contains("Remote_")).collect(Collectors.toList()); //get rid of DeltaRule and Remote_rule
        //由socialite执行表创建和非递归规则
        decls.forEach(clientEngine::run);
        for (Rule rule : rules) {
            L.info("exec: " + rule);
            if (!rule.isLeftRec()) {
                clientEngine.run(rule.getRuleText());
            } else {//process recursive rules
                asyncAnalysis.addRecRule(rule);
            }
        }
        asyncAnalysis.analysis();
        distAsyncCodeGen = new DistAsyncCodeGen(asyncAnalysis);
        for (String initRule : distAsyncCodeGen.generateInitStat()) {
            L.info("exec: " + initRule);
            clientEngine.run(initRule);
        }
//        clientEngine.run("?- AsyncTableSingle_sssp_mid(x, r, d, y).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        });
    }

    @Override
    public void run() {
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
        String tableSig = "Middle(int Key:0..875713, double initD, int degree, (int adj)).";
        clientEngine.run(tableSig);
        clientEngine.run("Middle(key, r, degree, adj) :- Rank(key, r), Edge(key, adj), EdgeCnt(key, degree).");

        IntStream.rangeClosed(1, workerNum).forEach(dest ->
            MPI.COMM_WORLD.Send(new byte[1], 0, 1, MPI.BYTE, dest, MsgType.NOTIFY_INIT.ordinal())
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


    public static void main(String[] args) {
        AsyncConfig asyncConfig = new AsyncConfig.Builder()
//                .setCheckInterval(1500)
                .setCheckerType(AsyncConfig.CheckerType.DELTA)
                .setCheckerCond(AsyncConfig.Cond.LE)
                .setThreshold(0.00001)
                .build();
        DistAsyncEngine distAsyncEngine = new DistAsyncEngine(TextUtils.readText(args[0]), 4);
    }
}
