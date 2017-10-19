package socialite.async.codegen;

import socialite.async.analysis.MyVisitorImpl;
import socialite.tables.QueryVisitor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class BaseAsyncTable {


    public abstract int getSize();

    public abstract void iterate(MyVisitorImpl visitor);

    public abstract void iterateTuple(QueryVisitor queryVisitor);

    public boolean updateLockFree(int localInd) {
        throw new NotImplementedException();
    }

    public abstract double getValue(int localInd);

    public abstract double getDelta(int localInd);

    public abstract double accumulateValue();

    public abstract double accumulateDelta();

    public abstract MyVisitorImpl getInitVisitor();

    public MyVisitorImpl getEdgeVisitor() {
        throw new NotImplementedException();
    }
}
