package socialite.async.codegen;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.atomic.MyAtomicDouble;
import socialite.engine.Config;
import socialite.util.Assert;
import socialite.util.AtomicDouble;
import socialite.util.MySTGroupFile;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class AsyncCodeGen {
    private AsyncAnalysis asyncAn;
    private AsyncConfig asyncConfig = AsyncConfig.get();

    public AsyncCodeGen(AsyncAnalysis asyncAn) {
        this.asyncAn = asyncAn;
    }

    String generateInitTable() {
        STGroup stg = new MySTGroupFile(AsyncCodeGen.class.getResource("AsyncTable.stg"),
                "UTF-8", '<', '>');
        stg.load();
        ST st = stg.getInstanceOf("InitTableStat");
        st.add("start",asyncAn.getRange()[0]);
        st.add("end",asyncAn.getRange()[1]);
        st.add("keyType", asyncAn.getKeyType());
        st.add("deltaType", asyncAn.getDeltaType());
        st.add("valueType", asyncAn.getValueType());
        st.add("weightType", asyncAn.getWeightType());
        st.add("extraType", asyncAn.getExtraType());
        st.add("recPName", asyncAn.getResultPName());
        st.add("edgePName", asyncAn.getEdgePName());
        st.add("extraPName", asyncAn.getExtraPName());
        st.add("dynamic", asyncConfig.isDynamic());
        return st.render();
    }

    String generateMessageTable(){
        STGroup stg = new MySTGroupFile(AsyncCodeGen.class.getResource("MessageTable.stg"),
                "UTF-8", '<', '>');
        stg.load();
        ST st = stg.getInstanceOf("MessageTable");
        st.add("keyType", asyncAn.getKeyType());
        st.add("deltaType", asyncAn.getDeltaType());
        st.add("aggrType", asyncAn.getAggrName());
        return st.render();
    }

    String generateAsyncTable() {
        STGroup stg = new MySTGroupFile(AsyncCodeGen.class.getResource("AsyncTable.stg"),
                "UTF-8", '<', '>');
        stg.load();
        ST st = stg.getInstanceOf("AsyncTableSharedMem");
        st.add("name", asyncAn.getResultPName());
        st.add("keyType", asyncAn.getKeyType());
        st.add("deltaType", asyncAn.getDeltaType());
        st.add("valueType", asyncAn.getValueType());
        st.add("aggrType", asyncAn.getAggrName());
        st.add("weightType", asyncAn.getWeightType());
        st.add("extraType", asyncAn.getExtraType());
        st.add("expr", asyncAn.getsExpr());
        st.add("dynamic", asyncConfig.isDynamic());
        return st.render();
    }

    String generateDistAsyncTable(){
        STGroup stg = new MySTGroupFile(AsyncCodeGen.class.getResource("AsyncTable.stg"),
                "UTF-8", '<', '>');
        stg.load();
        ST st = stg.getInstanceOf("DistAsyncTable");
        st.add("name", asyncAn.getResultPName());
        st.add("dynamic", asyncConfig.isDynamic());
        st.add("keyType", asyncAn.getKeyType());
        st.add("valueType", asyncAn.getValueType());
        st.add("deltaType", asyncAn.getDeltaType());
        st.add("aggrType", asyncAn.getAggrName());
        st.add("weightType", asyncAn.getWeightType());
        st.add("extraType", asyncAn.getExtraType());
        st.add("expr", asyncAn.getsExpr());
        return st.render();
    }
}
