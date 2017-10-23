package socialite.async.engine;

import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.codegen.AsyncCodeGenMain;
import socialite.async.codegen.AsyncRuntime;
import socialite.async.codegen.BaseAsyncTable;
import socialite.async.util.TextUtils;
import socialite.codegen.Analysis;
import socialite.engine.Config;
import socialite.engine.LocalEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;
import socialite.resource.TableInstRegistry;
import socialite.tables.QueryVisitor;
import socialite.tables.TableInst;
import socialite.tables.Tuple;
import socialite.util.SociaLiteException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public class LocalAsyncEngine {
    private AsyncAnalysis asyncAnalysis;
    private AsyncCodeGenMain asyncCodeGenMain;
    private LocalEngine localEngine;

    public LocalAsyncEngine(String program) {
        localEngine = new LocalEngine(Config.par(AsyncConfig.get().getThreadNum()));
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

    private void compile() {
        if (asyncAnalysis.analysis()) {
            if(AsyncConfig.get().isPriority()) {
                if (asyncAnalysis.getAggrName().equals("dcount") || asyncAnalysis.getAggrName().equals("dsum"))
                    AsyncConfig.get().setPriorityType(AsyncConfig.PriorityType.SUM_COUNT);
                else if (asyncAnalysis.getAggrName().equals("dmin"))
                    AsyncConfig.get().setPriorityType(AsyncConfig.PriorityType.MIN);
                else if (asyncAnalysis.getAggrName().equals("dmax"))
                    AsyncConfig.get().setPriorityType(AsyncConfig.PriorityType.MAX);
                else throw new SociaLiteException("unsupported priority");
            }

            asyncCodeGenMain = new AsyncCodeGenMain(asyncAnalysis);
            asyncCodeGenMain.generateSharedMem();
        }
    }

    private void run(QueryVisitor queryVisitor) {
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
            asyncTable.iterateTuple(queryVisitor);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        AsyncConfig asyncConfig = AsyncConfig.get();
        compile();
        List<String> initStats = asyncCodeGenMain.getInitStats();

        if (!asyncConfig.isDebugging()) {
            initStats.forEach(initStat -> localEngine.run(initStat));
            String savePath = AsyncConfig.get().getSavePath();
            TextUtils textUtils = null;
            if (savePath.length() > 0) {
                textUtils = new TextUtils(savePath, "part-0");
            }

            TextUtils finalTextUtils = textUtils;
            run(new QueryVisitor() {
                @Override
                public boolean visit(Tuple _0) {
                    if (asyncConfig.isPrintResult())
                        System.out.println(_0.toString());
                    if (finalTextUtils != null)
                        finalTextUtils.writeLine(_0.toString());
                    return true;
                }

                @Override
                public void finish() {
                    if (finalTextUtils != null)
                        finalTextUtils.close();
                }
            });//save result
        }
        localEngine.shutdown();
    }


}
