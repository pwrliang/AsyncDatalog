package socialite.async.codegen;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.AsyncAnalysis;
import socialite.codegen.Compiler;
import socialite.util.Loader;
import socialite.util.SociaLiteException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AsyncCodeGenMain {
    public static final String PACKAGE_NAME = AsyncCodeGenMain.class.getPackage().getName();
    private static final Log L = LogFactory.getLog(AsyncCodeGenMain.class);
    private AsyncAnalysis asyncAn;
    private Class<?> runtimeClass;
    private List<String> initStats;

    public AsyncCodeGenMain(AsyncAnalysis asyncAn) {
        this.asyncAn = asyncAn;
    }

    public void generate() {
        generateInitTableStats();
//        genAsyncTable();
//        L.info("AsyncTable compiled");
//        genRuntime();
//        L.info("AsyncRuntime compiled");
    }

    public void generateInitTableStats(){
        AsyncCodeGen asyncCodeGen = new AsyncCodeGen(asyncAn);
        String stats = asyncCodeGen.generateInitTable();
        String[] statArr = stats.replace("\r","").replace("\n","").split("\\$");
        initStats= Arrays.stream(statArr).map(String::trim).collect(Collectors.toList());
    }

    public void genAsyncTable() {
        AsyncCodeGen asyncCodeGen = new AsyncCodeGen(asyncAn);
        String asyncTableCode = asyncCodeGen.generateAsyncTable();
        String className = "AsyncTable";
        Compiler c = new Compiler();
        boolean success = c.compile(PACKAGE_NAME + "." + className, asyncTableCode);
        if (c.getCompiledClasses().size() == 0) {
            L.warn(PACKAGE_NAME + "." + className + " already compiled");

        }
        if (!success) {
            String msg = "Compilation error for " + className;
            msg += " " + c.getErrorMsg();
            throw new SociaLiteException(msg);
        }
    }

    public void genRuntime() {
        AsyncCodeGen asyncCodeGen = new AsyncCodeGen(asyncAn);
        String runtimeCode = asyncCodeGen.generateAsyncRuntime();
        String className = "AsyncRuntime";
        Compiler c = new Compiler();
        boolean success = c.compile(PACKAGE_NAME + "." + className, runtimeCode);
        if (c.getCompiledClasses().size() == 0) {
            L.warn(PACKAGE_NAME + "." + className + " already compiled");
        }
        if (!success) {
            String msg = "Compilation error for " + className;
            msg += " " + c.getErrorMsg();
            throw new SociaLiteException(msg);
        }
        runtimeClass = Loader.forName(PACKAGE_NAME + "." + className);
        if (runtimeClass == null)
            throw new SociaLiteException("Load Runtime Fail!!!");
    }

    public List<String> getInitStats() {
        return initStats;
    }

    public Class<?> getRuntimeClass() {
        return runtimeClass;
    }
}
