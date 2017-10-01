package socialite.async.engine;

import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.dist.ds.MsgType;
import socialite.async.dist.master.InitCarrier;
import socialite.async.util.SerializeTool;
import socialite.codegen.Analysis;
import socialite.engine.ClientEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistAsyncEngine implements Runnable {
    public static final double THRESHOLD = 0.9999;
    public static final int TERM_CHECK_INTERVAL = 2000;
    private static final Log L = LogFactory.getLog(DistAsyncEngine.class);
    private int workerNum;
    private StopWatch stopWatch;
    private ClientEngine clientEngine;

    public DistAsyncEngine(String program, int workerNum) {
        this.workerNum = workerNum;

        clientEngine = new ClientEngine();
        Parser parser = new Parser(program);
        parser.parse(program);
        Analysis tmpAn = new Analysis(parser);
        tmpAn.run();

        AsyncAnalysis asyncAnalysis = new AsyncAnalysis(tmpAn);
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


    public static final int INIT_CARRIER_INIT_SIZE = 200000; //init size 200k
    public static final int INIT_CARRIER_LOADED_THRESHOLD = (int) (INIT_CARRIER_INIT_SIZE * 1.5); //when reach the threshold, send it
    public static final int IDENTITY_ELEMENT = 0;//depending on algorithms


    private void loadData() {
        LinkedBlockingQueue<InitCarrier> initCarrierQueue = new LinkedBlockingQueue<>();
        InitCarrier[] initCarriers = IntStream.range(0, workerNum).
                mapToObj(workerId -> new InitCarrier(INIT_CARRIER_INIT_SIZE, workerId)).toArray(InitCarrier[]::new);

        Thread initThread = new Thread(() -> {
            InitCarrier carrier;
            SerializeTool serializeTool = new SerializeTool.Builder().build();
            try {
                while (true) {
                    carrier = initCarrierQueue.take();
                    if (carrier.getSize() == 0)//i got the marker element
                        break;
                    byte[] data = serializeTool.toBytes(carrier);
                    MPI.COMM_WORLD.Send(data, 0, data.length, MPI.BYTE, carrier.getWorkerId() + 1, MsgType.INIT_DATA.ordinal());
                }
                byte[] data = serializeTool.toBytes(carrier);//send marker element to all workers to notify all data sent.
                IntStream.rangeClosed(1, workerNum).parallel().forEach(machineId ->
                        MPI.COMM_WORLD.Send(data, 0, data.length, MPI.BYTE, machineId, MsgType.INIT_DATA.ordinal()));

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        initThread.start();
        clientEngine.run("Middle(int Key, double initD, int adj, int degree).");
        clientEngine.run("Middle(key, r, adj, degree) :- Rank(key, r), Edge(key, adj), EdgeCnt(key, degree).");
        clientEngine.run("?- Middle(key, r, adj, degree).", new QueryVisitor() {
            @Override
            public boolean visit(Tuple _0) {
                int key = _0.getInt(0);
                double delta = _0.getDouble(1);
                int adjacent = _0.getInt(2);
                int degree = _0.getInt(3);
                int workerId = InitCarrier.getWorkerId(key, workerNum);
                InitCarrier initCarrier = initCarriers[workerId];
                initCarrier.addEntry(key, IDENTITY_ELEMENT, delta, adjacent, degree);
                if (initCarrier.getSize() > INIT_CARRIER_LOADED_THRESHOLD) {
                    initCarrierQueue.add(initCarrier);
                    initCarriers[workerId] = new InitCarrier(INIT_CARRIER_INIT_SIZE, workerId);
                }
//                L.info(_0);
                return true;
            }
        }, 0);

        //send rest of init carriers
        Arrays.stream(initCarriers).filter(initCarrier -> initCarrier.getSize() > 0).forEach(initCarrierQueue::add);
        initCarrierQueue.add(new InitCarrier(0, 0));//add a marker element to notify exit
        try {
            initThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
