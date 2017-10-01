package socialite.async.atomic;

import java.util.concurrent.atomic.AtomicInteger;

public class MyAtomicInteger extends AtomicInteger {
    /**
     * Creates a new AtomicInteger with the given initial value.
     *
     * @param initialValue the initial value
     */
    public MyAtomicInteger(int initialValue) {
        super(initialValue);
    }

    /**
     * Creates a new AtomicInteger with initial value {@code 0}.
     */
    public MyAtomicInteger() {
        super();
    }
}
