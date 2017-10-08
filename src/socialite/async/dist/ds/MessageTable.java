package socialite.async.dist.ds;


import socialite.async.atomic.MyAtomicDouble;
import socialite.async.atomic.MyAtomicInteger;
import socialite.async.codegen.MessageTableBase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageTable extends MessageTableBase{
    public Map<Integer, MyAtomicDouble> keyDeltaMap;
    public transient MyAtomicInteger updateCounter;

    protected MessageTable(){
        //constructor for kryo
    }

    public MessageTable(int initSize) {
        updateCounter = new MyAtomicInteger(0);
        keyDeltaMap = new ConcurrentHashMap<>(initSize);

    }


    /**
     * 累积delta值
     *
     * @param key   全局索引
     * @param delta delta value
     */
    public void apply(int key, double delta) {
        MyAtomicDouble atomicDelta = keyDeltaMap.putIfAbsent(key, new MyAtomicDouble(delta));
        if (atomicDelta != null) {
            atomicDelta.accumulateAndGet(delta, Double::sum);
        }
        updateCounter.addAndGet(1);
    }

    public int getUpdateTimes() {
        return updateCounter.intValue();
    }

    public void resetDelta() {
        keyDeltaMap.values().forEach(delta -> delta.set(DistAsyncTable.IDENTITY_ELEMENT));
        updateCounter.set(0);
    }

    public Map getKeyDeltaMap() {
        return keyDeltaMap;
    }
}