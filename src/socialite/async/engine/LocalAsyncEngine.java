package socialite.async.engine;

import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.codegen.AsyncCodeGenMain;
import socialite.async.codegen.AsyncRuntime;
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
        if (!AsyncConfig.get().isDebugging())
            decls.forEach(localEngine::run);
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
                    localEngine.run(rule.getRuleText());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //worker number depends on ClusterConf
        //waiting all workers online
        AsyncConfig.parse(TextUtils.readText(args[0]));
        //prog9 volatile problem
        LocalAsyncEngine localAsyncEngine = new LocalAsyncEngine(AsyncConfig.get().getDatalogProg());
        localAsyncEngine.run();
    }

    private void compile() {
        if (asyncAnalysis.analysis()) {
            asyncCodeGenMain = new AsyncCodeGenMain(asyncAnalysis);
            asyncCodeGenMain.generateSharedMem();
        }
    }

    private void run(MyVisitorImpl myVisitor) {
        Analysis an = localEngine.getAn();
        TableInstRegistry registry = localEngine.getRuntime().getTableRegistry();
        TableInst[] recInst = registry.getTableInstArray(an.getTableMap().get("InitTable").id());
        TableInst[] edgeInst = registry.getTableInstArray(an.getTableMap().get(asyncAnalysis.getEdgePName()).id());

        Class<?> klass = asyncCodeGenMain.getAsyncTable();
        try {
            Constructor<?> constructor = klass.getConstructor(int.class);
            BaseAsyncTable asyncTable = (BaseAsyncTable) constructor.newInstance(AsyncConfig.get().getInitSize());
            AsyncRuntime asyncRuntime = new AsyncRuntime(asyncTable, recInst, edgeInst);
            asyncRuntime.run();
            asyncTable.iterate(myVisitor);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
        }

//        try {
//            TableInst[] insts = new TableInst[0];
//            Constructor<?> constructor = klass.getDeclaredConstructor(localEngine.getClass(), insts.getClass(), insts.getClass());
//            AsyncRuntimeBase asyncRuntime = (AsyncRuntimeBase) constructor.newInstance(localEngine, recInst, edgeInst);
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
        if (!AsyncConfig.get().isDebugging()) {
            initStats.forEach(initStat -> localEngine.run(initStat));
            run(new MyVisitorImpl() {

                @Override
                public boolean visit(int a1, double a2, double a3) {
                    System.out.println(a1 + " " + a2 + " " + a3);
                    return true;
                }

                //CC
                @Override
                public boolean visit(int a1, int a2, int a3) {
                    System.out.println(a1 + " " + a2 + " " + a3);
                    return true;
                }

                //COUNT PATH IN DAG
                @Override
                public boolean visit(Object a1, int a2, int a3) {
                    System.out.println(a1 + " " + a2 + " " + a3);
                    return true;
                }

                //PARTY
                @Override
                public boolean visit(int a1) {
                    System.out.println(a1);
                    return true;
                }
            });
        }
        localEngine.shutdown();
    }


}
