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

    public void generateSharedMem() {
        genInitTableStats();
        compileAsyncTable();
        L.info("AsyncTable compiled");
        compileRuntime();
        L.info("AsyncRuntime compiled");
    }

    public void generateDist() {
        compileMessageTable();
        L.info("MessageTable compiled");
        compileDistAsyncTable();
        L.info("DistAsyncTable compiled");
    }

    public void compileMessageTable() {
        AsyncCodeGen asyncCodeGen = new AsyncCodeGen(asyncAn);
        String messageTableCode = asyncCodeGen.generateMessageTable();
        String className = "MessageTable";
        Compiler c = new Compiler();
        boolean success = c.compile(PACKAGE_NAME + "." + className, messageTableCode);
        if (c.getCompiledClasses().size() == 0) {
            L.warn(PACKAGE_NAME + "." + className + " already compiled");

        }
        if (!success) {
            String msg = "Compilation error for " + className;
            msg += " " + c.getErrorMsg();
            throw new SociaLiteException(msg);
        }
    }

    public void compileDistAsyncTable() {
        AsyncCodeGen asyncCodeGen = new AsyncCodeGen(asyncAn);
        String distAsyncTableCode = asyncCodeGen.generateDistAsyncTable();
        String className = "DistAsyncTable";
        Compiler c = new Compiler();
        boolean success = c.compile(PACKAGE_NAME + "." + className, distAsyncTableCode);
        if (c.getCompiledClasses().size() == 0) {
            L.warn(PACKAGE_NAME + "." + className + " already compiled");

        }
        if (!success) {
            String msg = "Compilation error for " + className;
            msg += " " + c.getErrorMsg();
            throw new SociaLiteException(msg);
        }
    }

    public void genInitTableStats() {
        AsyncCodeGen asyncCodeGen = new AsyncCodeGen(asyncAn);
        String stats = asyncCodeGen.generateInitTable();
        String[] statArr = stats.replace("\r", "").replace("\n", "").split("\\$");
        initStats = Arrays.stream(statArr).map(String::trim).collect(Collectors.toList());
    }

    private void compileAsyncTable() {
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

    private void compileRuntime() {
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

    private void compileDistRuntime() {

    }

    public List<String> getInitStats() {
        return initStats;
    }

    public Class<?> getRuntimeClass() {
        return runtimeClass;
    }
}
