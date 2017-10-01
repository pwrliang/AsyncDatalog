package socialite.async.atomic;


import com.google.common.util.concurrent.AtomicDouble;

import java.util.function.DoubleBinaryOperator;

public class MyAtomicDouble extends AtomicDouble {
    public MyAtomicDouble() {
        this(0.0d);
    }

    public MyAtomicDouble(double initialValue) {
        super(initialValue);
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
    public final double accumulateAndGet(double x,
                                         DoubleBinaryOperator accumulatorFunction) {
        double prev, next;
        do {
            prev = get();
            next = accumulatorFunction.applyAsDouble(prev, x);
        } while (!compareAndSet(prev, next));
        return next;
    }
}
