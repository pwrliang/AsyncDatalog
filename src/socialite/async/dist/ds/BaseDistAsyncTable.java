package socialite.async.dist.ds;


import socialite.async.analysis.MyVisitorImpl;
import socialite.async.codegen.BaseAsyncTable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class BaseDistAsyncTable extends BaseAsyncTable {
    public abstract MessageTable[] getMessageAndBackup(int workerId);

    public abstract AtomicIntegerArray getMessageTableSelector();

    public abstract void applyBuffer(MessageTable messageTable);

    public abstract Object accumulateValue();

    public MyVisitorImpl getEdgeVisitor() {
        throw new NotImplementedException();
    }

    public MyVisitorImpl getExtraVisitor() {
        throw new NotImplementedException();
    }

    public MyVisitorImpl getInitVisitor() {
        throw new NotImplementedException();
    }
}
