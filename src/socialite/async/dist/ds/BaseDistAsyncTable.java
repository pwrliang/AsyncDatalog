package socialite.async.dist.ds;


import socialite.async.analysis.MyVisitorImpl;
import socialite.async.codegen.BaseAsyncTable;
import socialite.async.codegen.MessageTableBase;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class BaseDistAsyncTable extends BaseAsyncTable {
    public abstract MessageTableBase[] getMessageTables(int workerId);

    public abstract AtomicIntegerArray getMessageTableSelector();

    public abstract void applyBuffer(MessageTableBase messageTable);

    public abstract Object accumulateValue();

    public MyVisitorImpl getEdgeVisitor() {
        throw new NotImplementedException();
    }

    public MyVisitorImpl getInitVisitor() {
        throw new NotImplementedException();
    }
}
