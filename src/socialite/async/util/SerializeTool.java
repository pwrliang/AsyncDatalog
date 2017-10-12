package socialite.async.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class SerializeTool {
    private static final Log L = LogFactory.getLog(SerializeTool.class);
    private Kryo kryo;
    private int initSize;

    private SerializeTool() {
    }


    //    Kryo has three sets of methods for reading and writing objects.
    //
    //    If the concrete class of the object is not known and the object could be null:
    //
    //            kryo.writeClassAndObject(output, object);
    //    // ...
    //    Object object = kryo.readClassAndObject(input);
    //    if (object instanceof SomeClass) {
    //        // ...
    //    }
    //    If the class is known and the object could be null:
    //
    //            kryo.writeObjectOrNull(output, someObject);
    //    // ...
    //    SomeClass someObject = kryo.readObjectOrNull(input, SomeClass.class);
    //    If the class is known and the object cannot be null:
    //
    //            kryo.writeObject(output, someObject);
    //    // ...
    //    SomeClass someObject = kryo.readObject(input, SomeClass.class);

//    public ByteBuffer toByteBuffer(Object object) {
//        return toByteBuffer(initSize, object);
//    }
//
//    public ByteBuffer toByteBuffer(int buffSize, Object object) {
//        ByteBuffer byteBuffer = ByteBuffer.allocate(buffSize);
//        ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream(byteBuffer);
//        Output output = new Output(byteBufferOutputStream);
//        kryo.writeObject(output, object);
//        output.close();
//        return byteBuffer;
//    }

    public static void main(String[] args) {
//        SerializeTool serializeTool = new SerializeTool.Builder().setInitSize(128 * 1024 * 1024).registry(Test.class).registry(ArrayList.class).build();
//        Test test = new Test();
//        test.set();
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        byte[] data = new byte[0];
//        for (int i = 0; i < 10; i++) {
//            data = serializeTool.toBytes(test);
//            test = serializeTool.fromBytes(data, Test.class);
//            System.out.println(data.length);
//            ByteBuffer byteBuffer = serializeTool.toByteBuffer(test);
//            System.out.println(byteBuffer.position());
//            test = serializeTool.fromByteBuffer(byteBuffer, Test.class);
//        }
//        stopWatch.stop();
//        System.out.println("size " + data.length / 1024 / 1024);
//        System.out.println(stopWatch.getTime() / 10);
//        System.out.println(test.data.get(0));
//        ByteBuffer byteBuffer = ;

        //  System.out.println(serializeTool.fromByteBuffer(serializeTool.toByteBuffer(test), Test.class).i);
    }

//    public ByteBuffer MPI_toByteBuffer(int buffSize, Object object) throws MPIException {
//        ByteBuffer byteBuffer = MPI.newByteBuffer(buffSize);
//        ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream(byteBuffer);
//        Output output = new Output(byteBufferOutputStream);
//        kryo.writeObject(output, object);
//        output.close();
//        return byteBuffer;
//    }
//
//    public <T> T MPI_fromByteBuffer(ByteBuffer byteBuffer, Class<T> klass) {
//        Input input = new Input(new ByteBufferInputStream(byteBuffer));
//        T object = kryo.readObject(input, klass);
//        input.close();
//        return object;
//    }
//
//    public <T> T fromByteBuffer(ByteBuffer byteBuffer, Class<T> klass) {
//        Input input = new Input(byteBuffer.array());
//        T object = kryo.readObject(input, klass);
//        input.close();
//        return object;
//    }

    public byte[] toBytes(int buffSize, Object object) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(buffSize);
        Output output = new Output(byteArrayOutputStream);
        kryo.writeObject(output, object);
        output.close();
        return byteArrayOutputStream.toByteArray();
    }

    public byte[] toBytes(Object object) {
        return toBytes(initSize, object);
    }


    public <T> T fromBytes(byte[] data, Class<T> klass) {
        Input input = new Input(new ByteArrayInputStream(data));
        T object = kryo.readObject(input, klass);
        input.close();
        return object;
    }

    public Object fromBytesToObject(byte[] data, Class<?> klass) {
        Input input = new Input(new ByteArrayInputStream(data));
        Object object = kryo.readObject(input, klass);
        input.close();
        return object;
    }

    public Object fromBytes(byte[] data) {
        Input input = new Input(new ByteArrayInputStream(data));
        Object object = kryo.readClassAndObject(input);
        input.close();
        return object;
    }
    /*
    public byte[] toBytes(Object object) {
        ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream(initSize);
        Output output = new Output(byteBufferOutputStream);
        kryo.writeObject(output, object);
        output.close();
        return byteBufferOutputStream.getByteBuffer().array();
    }



    public <T> T fromBytes(byte[] data, Class<T> klass) {
        Input input = new Input(new ByteBufferInputStream(ByteBuffer.wrap(data)));
        T object = kryo.readObject(input, klass);
        input.close();
        return object;
    }
    */

    public static class Builder {
        Kryo kryo;
        int initSize = 32 * 1024 * 1024;

        public Builder() {
            kryo = new Kryo();
        }

        public Builder setBufferSize(int initSize) {
            this.initSize = initSize;
            return this;
        }

        /**
         * 注册被序列、反序列化的类，序列化和反序列化需要相同的注册顺序
         *
         * @param klass
         * @return
         */
        public Builder registry(Class<?> klass) {
            kryo.register(klass);
            kryo.setRegistrationRequired(true);
            return this;
        }

        public Builder setSerializeTransient(boolean enable) {
            kryo.getFieldSerializerConfig().setSerializeTransient(enable);
            return this;
        }

        public Builder setIgnoreSyntheticFields(boolean ignore) {
            kryo.getFieldSerializerConfig().setIgnoreSyntheticFields(ignore);
            return this;
        }

        public Builder setInitSize(int initSize) {
            this.initSize = initSize;
            return this;
        }

        public SerializeTool build() {
            SerializeTool serializeTool = new SerializeTool();
            serializeTool.kryo = kryo;
            serializeTool.initSize = initSize;
            return serializeTool;
        }
    }

    private static class Test {
        List<String> data;

        void set() {
            data = new ArrayList<>();
            for (int i = 0; i < 3 * 1000 * 1000; i++) {
                data.add(i + "abcdefghijklmnopqrstuvwxyz");
            }
        }
    }
}
