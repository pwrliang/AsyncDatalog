package socialite.async.codegen;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.engine.Config;
import socialite.util.Assert;
import socialite.util.MySTGroupFile;

import java.util.ArrayList;
import java.util.List;

public class AsyncCodeGen {
    private AsyncAnalysis asyncAn;
    private AsyncConfig asyncConfig = AsyncConfig.get();

    public AsyncCodeGen(AsyncAnalysis asyncAn) {
        this.asyncAn = asyncAn;
    }

    String generateAsyncTable() {
        STGroup stg = new MySTGroupFile(AsyncCodeGen.class.getResource("AsyncTable.stg"),
                "UTF-8", '<', '>');
        stg.load();
        ST st;
        if (asyncAn.isPairKey()) {
            st = stg.getInstanceOf("AsyncTablePair");
            st.add("name", asyncAn.getClassName());
            st.add("valueType", asyncAn.getValueType());
            st.add("aggrType", asyncAn.getAggrName());
            st.add("weightV", asyncAn.getWeightV());
            st.add("edgeIsNested", asyncAn.edgePIsNested());
//            st.add("extraV", asyncAn.extra);
            st.add("expr", asyncAn.getsExpr());
        } else {
            if (asyncAn.isTwoStep()) {//two step
                st = stg.getInstanceOf("AsyncTableSingleTwoStep");
                st.add("name", asyncAn.getClassName());
                st.add("valueType", asyncAn.getValueType());
                st.add("deltaType", asyncAn.getDeltaType());
                st.add("aggrType", asyncAn.getAggrName());
                st.add("srcV", asyncAn.getSrcV().get(0));
                st.add("dstV", asyncAn.getDstV().get(0));
                st.add("edgeIsNested", asyncAn.edgePIsNested());
                st.add("expr", asyncAn.getsExpr());
            } else {
                st = stg.getInstanceOf("AsyncTableSingle");
                st.add("name", asyncAn.getClassName());
                st.add("valueType", asyncAn.getValueType());
                st.add("aggrType", asyncAn.getAggrName());
                st.add("srcV", asyncAn.getSrcV().get(0));
                st.add("dstV", asyncAn.getDstV().get(0));
                st.add("weightV", asyncAn.getWeightV());
                st.add("edgeIsNested", asyncAn.edgePIsNested());
                st.add("extraV", asyncAn.getExtra());
                st.add("expr", asyncAn.getsExpr());
            }
        }
        String code = st.render();
        stg.unload();
        return code;
    }

    public String generateAsyncRuntime() {

        STGroup stg = new MySTGroupFile(AsyncCodeGen.class.getResource("AsyncRuntime.stg"),
                "UTF-8", '<', '>');
        stg.load();
        ST st = stg.getInstanceOf("AsyncRuntime");
        st.add("AsyncTableClass", asyncAn.getClassName());
        st.add("valueType", asyncAn.getValueType());
        st.add("initSize", asyncAn.getInitSize());
        st.add("threadNum", Config.getInst().getWorkerThreadNum());// standalone
        st.add("dynamic", asyncAn.isPairKey() || asyncAn.isTwoStep());
        st.add("threshold", asyncConfig.getThreshold());
        st.add("cond", asyncConfig.getCond());
        st.add("barrier", asyncConfig.isBarrier());
        st.add("checkerInterval", asyncConfig.getCheckInterval());
        if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA)
            st.add("checkType", "CheckDelta");
        else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE)
            st.add("checkType", "CheckValue");
        else
            Assert.not_supported();
        String code = st.render();
        stg.unload();
        return code;
    }
}
