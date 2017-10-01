package socialite.async.analysis;

import socialite.util.Assert;
import socialite.visitors.VisitorImpl;

public abstract class MyVisitorImpl extends VisitorImpl {
    @Override
    public int getEpochId() {
        return 0;
    }

    @Override
    public int getRuleId() {
        return 0;
    }

    public boolean visit(int a0, boolean a1) {
        Assert.not_implemented();
        return false;
    }
}
