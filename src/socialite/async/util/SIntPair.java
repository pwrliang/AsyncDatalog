package socialite.async.util;

public class SIntPair {
    private int val0;
    private int val1;

    private SIntPair() {

    }

    public SIntPair(int val0, int val1) {
        this.val0 = val0;
        this.val1 = val1;
    }

    public int getValue0() {
        return val0;
    }

    public int getValue1() {
        return val1;
    }

    @Override
    public String toString() {
        return String.format("(%d, %d)", val0, val1);
    }
}
