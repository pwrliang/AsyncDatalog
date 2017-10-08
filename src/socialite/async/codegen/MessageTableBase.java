package socialite.async.codegen;


import socialite.async.atomic.MyAtomicDouble;
import socialite.async.atomic.MyAtomicInteger;
import socialite.async.dist.ds.DistAsyncTable;
import socialite.async.dist.ds.MessageTable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class MessageTableBase {
    transient MyAtomicInteger updateCounter;

    public int getUpdateTimes() {
        return updateCounter.intValue();
    }

    public abstract void resetDelta() ;

    public abstract Map getKeyDeltaMap();
}