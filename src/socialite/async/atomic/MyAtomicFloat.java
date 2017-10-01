package socialite.async.atomic;

import java.util.concurrent.atomic.AtomicInteger;

public class MyAtomicFloat extends Number {
    private static final long serialVersionUID = 12327722191124184L;

    private final AtomicInteger bits;

    public MyAtomicFloat() {
        this(0.0f);
    }

    public MyAtomicFloat(float initialValue) {
        bits = new AtomicInteger(toInteger(initialValue));
    }

    private static float toFloat(int i) {
        return Float.intBitsToFloat(i);
    }

    private static int toInteger(float delta) {
        return Float.floatToIntBits(delta);
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public final boolean compareAndSet(float expect, float update) {
        return bits.compareAndSet(toInteger(expect), toInteger(update));
    }

    /**
     * Sets to the given value.
     *
     * @param newValue the new value
     */
    public final void set(float newValue) {
        bits.set(toInteger(newValue));
    }

    public final float get() {
        return toFloat(bits.get());
    }

    /**
     * Atomically sets to the given value and returns the old value.
     *
     * @param newValue the new value
     * @return the previous value
     */
    public final float getAndSet(float newValue) {
        return toFloat(bits.getAndSet(toInteger(newValue)));
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     * <p>
     * <p><a href="package-summary.html#weakCompareAndSet">May fail
     * spuriously and does not provide ordering guarantees</a>, so is
     * only rarely an appropriate alternative to {@code compareAndSet}.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful
     */
    public final boolean weakCompareAndSet(float expect, float update) {
        return bits.weakCompareAndSet(toInteger(expect), toInteger(update));
    }

    /**
     * Atomically updates the current value with the results of
     * applying the given function to the current and given values,
     * returning the updated value. The function should be
     * side-effect-free, since it may be re-applied when attempted
     * updates fail due to contention among threads.  The function
     * is applied with the current value as its first argument,
     * and the given update as the second argument.
     *
     * @param x                   the update value
     * @param accumulatorFunction a side-effect-free function of two arguments
     * @return the updated value
     * @since 1.8
     */
    public final float accumulateAndGet(float x, FloatBinaryOperator accumulatorFunction) {
        float prev, next;
        do {
            prev = get();
            next = accumulatorFunction.applyAsFloat(prev, x);
        } while (!compareAndSet(prev, next));
        return next;
    }

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    public final float addAndGet(float delta) {
        return toFloat(bits.addAndGet(toInteger(delta)));
    }

    /**
     * Atomically decrements by one the current value.
     *
     * @return the updated value
     */
    public final float decrementAndGet() {
        return addAndGet(-1.0f);
    }

    /**
     * Atomically updates the current value with the results of
     * applying the given function to the current and given values,
     * returning the previous value. The function should be
     * side-effect-free, since it may be re-applied when attempted
     * updates fail due to contention among threads.  The function
     * is applied with the current value as its first argument,
     * and the given update as the second argument.
     *
     * @param x                   the update value
     * @param accumulatorFunction a side-effect-free function of two arguments
     * @return the previous value
     * @since 1.8
     */
    public final float getAndAccumulate(float x, FloatBinaryOperator accumulatorFunction) {
        float prev, next;
        do {
            prev = get();
            next = accumulatorFunction.applyAsFloat(prev, x);
        } while (!compareAndSet(prev, next));
        return prev;
    }

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the previous value
     */
    public final float getAndAdd(float delta) {
        return toFloat(bits.getAndAdd(toInteger(delta)));
    }

    public final float getAndDecrement() {
        return getAndAdd(-1.0f);
    }

    /**
     * Atomically increments by one the current value.
     *
     * @return the previous value
     */
    public final float getAndIncrement() {
        return getAndAdd(1.0f);
    }

    /**
     * Atomically increments by one the current value.
     *
     * @return the updated value
     */
    public final float incrementAndGet() {
        return addAndGet(1.0f);
    }

    /**
     * Atomically updates the current value with the results of
     * applying the given function, returning the previous value. The
     * function should be side-effect-free, since it may be re-applied
     * when attempted updates fail due to contention among threads.
     *
     * @param updateFunction a side-effect-free function
     * @return the previous value
     * @since 1.8
     */
    public final float getAndUpdate(FloatUnaryOperator updateFunction) {
        float prev, next;
        do {
            prev = get();
            next = updateFunction.applyAsFloat(prev);
        } while (!compareAndSet(prev, next));
        return prev;
    }

    /**
     * Eventually sets to the given value.
     *
     * @param newValue the new value
     * @since 1.6
     */
    public final void lazySet(float newValue) {
        bits.lazySet(toInteger(newValue));
        // unsafe.putOrderedLong(this, valueOffset, newValue);
    }

    /**
     * Returns the value of this {@code AtomicLong} as a {@code long}.
     */
    public long longValue() {
        return (long) get();
    }

    /**
     * Returns the String representation of the current value.
     *
     * @return the String representation of the current value
     */
    public String toString() {
        return Float.toString(get());
    }

    /**
     * Atomically updates the current value with the results of
     * applying the given function, returning the updated value. The
     * function should be side-effect-free, since it may be re-applied
     * when attempted updates fail due to contention among threads.
     *
     * @param updateFunction a side-effect-free function
     * @return the updated value
     * @since 1.8
     */
    public final float updateAndGet(FloatUnaryOperator updateFunction) {
        float prev, next;
        do {
            prev = get();
            next = updateFunction.applyAsFloat(prev);
        } while (!compareAndSet(prev, next));
        return next;
    }

    /**
     * Returns the value of this {@code AtomicLong} as an {@code int}
     * after a narrowing primitive conversion.
     *
     * @jls 5.1.3 Narrowing Primitive Conversions
     */
    public int intValue() {
        return (int) get();
    }

    /**
     * Returns the value of this {@code AtomicLong} as a {@code float}
     * after a widening primitive conversion.
     *
     * @jls 5.1.2 Widening Primitive Conversions
     */
    public float floatValue() {
        return get();
    }

    /**
     * Returns the value of this {@code AtomicLong} as a {@code float}
     * after a widening primitive conversion.
     *
     * @jls 5.1.2 Widening Primitive Conversions
     */
    public double doubleValue() {
        return get();
    }
}
