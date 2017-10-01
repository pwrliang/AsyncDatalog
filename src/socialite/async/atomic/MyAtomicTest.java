package socialite.async.atomic;

public class MyAtomicTest {
    public static void main(String[] args) {
        Double d = 0.2;
        MyAtomicDouble myAtomicDouble = new MyAtomicDouble(0.2);
        myAtomicDouble.accumulateAndGet(0.3, Math::max);
        System.out.println(myAtomicDouble);
    }
}
