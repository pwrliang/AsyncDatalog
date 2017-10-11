package socialite.async.codegen;

import socialite.async.analysis.MyVisitorImpl;
import socialite.tables.QueryVisitor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class BaseAsyncTable {


    public abstract int getSize();

    public abstract void iterate(MyVisitorImpl visitor);

    public abstract void iterateTuple(QueryVisitor queryVisitor);

    public void updateLockFree(int localInd) {
        throw new NotImplementedException();
    }

    public abstract Object accumulateValue();

    public abstract Object accumulateDelta();

    public abstract MyVisitorImpl getInitVisitor();

    public MyVisitorImpl getEdgeVisitor() {
        throw new NotImplementedException();
    }
}
