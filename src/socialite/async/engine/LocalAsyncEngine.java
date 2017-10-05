package socialite.async.engine;

import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.codegen.AsyncCodeGenMain;
import socialite.async.codegen.AsyncRuntimeBase;
import socialite.async.codegen.BaseAsyncTable;
import socialite.async.util.TextUtils;
import socialite.codegen.Analysis;
import socialite.engine.LocalEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;
import socialite.resource.TableInstRegistry;
import socialite.tables.TableInst;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public class LocalAsyncEngine {
    private AsyncAnalysis asyncAnalysis;
    private AsyncCodeGenMain asyncCodeGenMain;
    private LocalEngine localEngine;

    LocalAsyncEngine(String program) {
        localEngine = new LocalEngine();
        Parser parser = new Parser(program);
        parser.parse(program);
        Analysis tmpAn = new Analysis(parser);
        tmpAn.run();
        asyncAnalysis = new AsyncAnalysis(tmpAn);
        List<String> decls = parser.getTableDeclMap().values().stream().map(TableDecl::getDeclText).collect(Collectors.toList());
        List<Rule> rules = tmpAn.getEpochs().stream().flatMap(epoch -> epoch.getRules().stream()).filter(rule -> !(rule instanceof DeltaRule)).collect(Collectors.toList());
        //由socialite执行表创建和非递归规则
        decls.forEach(localEngine::run);
        for (Rule rule : rules) {
            if (!rule.isLeftRec()) {
//                localEngine.run(rule.getRuleText());
            } else {//process recursive rules
                asyncAnalysis.addRecRule(rule);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //worker number depends on ClusterConf
        //waiting all workers online
        AsyncConfig asyncConfig = new AsyncConfig.Builder()
                .setCheckInterval(1500)
                .setCheckerType(AsyncConfig.CheckerType.DELTA)
                .setCheckerCond(AsyncConfig.Cond.LE)
                .setThreshold(0.00001)
                .build();
        LocalAsyncEngine localAsyncEngine = new LocalAsyncEngine(TextUtils.readText(args[0]));
        localAsyncEngine.run();


//        ClientEngine clientEngine = new ClientEngine();
//        decls.forEach(clientEngine::run);//create tables
//        clientEngine.shutdown();
    }

    private void compile() {
        if (asyncAnalysis.analysis()) {
            asyncCodeGenMain = new AsyncCodeGenMain(asyncAnalysis);
            asyncCodeGenMain.generate();
        }
    }

    private void run(MyVisitorImpl myVisitor) {
//        Analysis an = localEngine.getAn();
//        TableInstRegistry registry = localEngine.getRuntime().getTableRegistry();
//        TableInst[] recInst = registry.getTableInstArray(an.getTableMap().get(asyncAnalysis.getResultPName()).id());
//        TableInst[] edgeInst = registry.getTableInstArray(an.getTableMap().get(asyncAnalysis.getEdgePName()).id());
//        TableInst[] extraInst = null;
//        if (asyncAnalysis.getExtra() != null)
//            extraInst = registry.getTableInstArray(an.getTableMap().get(asyncAnalysis.getExtraPName()).id());
//        Class<?> klass = asyncCodeGenMain.getRuntimeClass();
//        try {
//            TableInst[] insts = new TableInst[0];
//            Constructor<?> constructor = klass.getDeclaredConstructor(localEngine.getClass(), insts.getClass(), insts.getClass(), insts.getClass());
//            AsyncRuntimeBase asyncRuntime = (AsyncRuntimeBase) constructor.newInstance(localEngine, recInst, edgeInst, extraInst);
//            asyncRuntime.run();
//            BaseAsyncTable asyncTable = asyncRuntime.getAsyncTable();
//            asyncTable.iterate(myVisitor);
//        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
//            e.printStackTrace();
//        }
    }

    public void run() {
        compile();
        List<String> initStats = asyncCodeGenMain.getInitStats();
        for(String stat:initStats) {
            localEngine.run(stat);
            System.out.println(stat);
        }
//        run(new MyVisitorImpl() {
//            @Override
//            public boolean visit(int a1, int a2) {
//                System.out.println(a1 + " " + a2);
//                return true;
//            }
//        });
        localEngine.shutdown();
    }


}
