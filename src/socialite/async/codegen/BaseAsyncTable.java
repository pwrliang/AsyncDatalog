package socialite.async.codegen;

import socialite.async.analysis.MyVisitorImpl;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class BaseAsyncTable {
    public abstract int getSize();

    public abstract void iterate(MyVisitorImpl visitor);

    public void updateLockFree(int k) {
        throw new NotImplementedException();
    }

    public void updateLockFree(long k) {
        throw new NotImplementedException();
    }

    public abstract Object getValue();

    public abstract MyVisitorImpl getEdgeVisitor();

    public abstract MyVisitorImpl getExtraVisitor();

    public abstract MyVisitorImpl getInitVisitor();

}
