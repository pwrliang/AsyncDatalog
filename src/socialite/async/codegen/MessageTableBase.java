package socialite.async.codegen;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.atomic.MyAtomicDouble;
import socialite.async.atomic.MyAtomicFloat;
import socialite.async.atomic.MyAtomicInteger;
import socialite.async.atomic.MyAtomicLong;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Map;

public abstract class MessageTableBase {
    protected static final Log L = LogFactory.getLog(MessageTableBase.class);
    protected transient MyAtomicInteger updateCounter;

    protected MessageTableBase() {
        updateCounter = new MyAtomicInteger(0);
    }


    public int getUpdateTimes() {
        return updateCounter.intValue();
    }

    public void apply(int key, int delta) {
        throw new NotImplementedException();
    }

    public void apply(int key, float delta) {
        throw new NotImplementedException();
    }

    public void apply(int key, long delta) {
        throw new NotImplementedException();
    }

    public void apply(int key, double delta) {
        throw new NotImplementedException();
    }

    public void apply(float key, int delta) {
        throw new NotImplementedException();
    }

    public void apply(float key, float delta) {
        throw new NotImplementedException();
    }

    public void apply(float key, long delta) {
        throw new NotImplementedException();
    }

    public void apply(float key, double delta) {
        throw new NotImplementedException();
    }

    public void apply(long key, int delta) {
        throw new NotImplementedException();
    }

    public void apply(long key, float delta) {
        throw new NotImplementedException();
    }

    public void apply(long key, long delta) {
        throw new NotImplementedException();
    }

    public void apply(long key, double delta) {
        throw new NotImplementedException();
    }

    public void apply(double key, int delta) {
        throw new NotImplementedException();
    }

    public void apply(double key, float delta) {
        throw new NotImplementedException();
    }

    public void apply(double key, long delta) {
        throw new NotImplementedException();
    }

    public void apply(double key, double delta) {
        throw new NotImplementedException();
    }

    public void apply(Pair key, int delta) {
        throw new NotImplementedException();
    }

    public void apply(Pair key, float delta) {
        throw new NotImplementedException();
    }

    public void apply(Pair key, long delta) {
        throw new NotImplementedException();
    }

    public void apply(Pair key, double delta) {
        throw new NotImplementedException();
    }


    public Map<Integer, MyAtomicInteger> getIntegerIntegerMap() {
        throw new NotImplementedException();
    }

    public Map<Integer, MyAtomicFloat> getIntegerFloatMap() {
        throw new NotImplementedException();
    }

    public Map<Integer, MyAtomicLong> getIntegerLongMap() {
        throw new NotImplementedException();
    }

    public Map<Integer, MyAtomicDouble> getIntegerDoubleMap() {
        throw new NotImplementedException();
    }

    public Map<Float, MyAtomicInteger> getFloatIntegerMap() {
        throw new NotImplementedException();
    }

    public Map<Float, MyAtomicFloat> getFloatFloatMap() {
        throw new NotImplementedException();
    }

    public Map<Float, MyAtomicLong> getFloatLongMap() {
        throw new NotImplementedException();
    }

    public Map<Float, MyAtomicDouble> getFloatDoubleMap() {
        throw new NotImplementedException();
    }

    public Map<Long, MyAtomicInteger> getLongIntegerMap() {
        throw new NotImplementedException();
    }

    public Map<Long, MyAtomicFloat> getLongFloatMap() {
        throw new NotImplementedException();
    }

    public Map<Long, MyAtomicLong> getLongLongMap() {
        throw new NotImplementedException();
    }

    public Map<Long, MyAtomicDouble> getLongDoubleMap() {
        throw new NotImplementedException();
    }

    public Map<Double, MyAtomicInteger> getDoubleIntegerMap() {
        throw new NotImplementedException();
    }

    public Map<Double, MyAtomicFloat> getDoubleFloatMap() {
        throw new NotImplementedException();
    }

    public Map<Double, MyAtomicLong> getDoubleLongMap() {
        throw new NotImplementedException();
    }

    public Map<Double, MyAtomicDouble> getDoubleDoubleMap() {
        throw new NotImplementedException();
    }

    public Map<Pair, MyAtomicInteger> getPairIntegerMap() {
        throw new NotImplementedException();
    }

    public Map<Pair, MyAtomicFloat> getPairFloatMap() {
        throw new NotImplementedException();
    }

    public Map<Pair, MyAtomicLong> getPairLongMap() {
        throw new NotImplementedException();
    }

    public Map<Pair, MyAtomicDouble> getPairDoubleMap() {
        throw new NotImplementedException();
    }

    public abstract double accumulate();

    public abstract int size();

    public abstract void resetDelta();

    public static void main(String[] args) {
        String[] keys = {"Integer", "Float", "Long", "Double", "Pair"};
        String[] vals = {"Integer", "Float", "Long", "Double"};
        for (String key : keys)
            for (String val : vals) {
                String code = String.format("public Map<%s, MyAtomic%s> get%s%sMap() {throw new NotImplementedException();}",
                        key, val, key, val);
                System.out.println(code);
            }
    }
}