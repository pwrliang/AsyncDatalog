package socialite.async.codegen;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.atomic.MyAtomicInteger;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;
import socialite.tables.Tuple_int;
import socialite.visitors.VisitorImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

//algorithm coming
public class AsyncBak extends BaseAsyncTable {
    //    Map<Integer, Integer> keyIndMap;
    TIntIntMap keyIndMap;
    TIntList indKeyList;
    List<Boolean> value;
    List<AtomicInteger> deltaValue;
    List<TIntList> data;
    TIntObjectMap<TIntList> bucket;//src -> dst1,2,3...
    List<Boolean> filled;
    List<Boolean> send;


    public static final int IDENTITY_ELEMENT = 0;
    private int size;

    public AsyncBak(int initSize) {
        keyIndMap = new TIntIntHashMap(initSize);
        indKeyList = new TIntArrayList(initSize);
        value = new ArrayList<>(initSize);
        deltaValue = new ArrayList<>(initSize);
        data = new ArrayList<>(initSize);
        bucket = new TIntObjectHashMap<>(initSize);
        filled = new ArrayList<>();
        send = new ArrayList<>(initSize);
    }

    void update_lockfree(int ind) {
        int oldDelta = deltaValue.get(ind).get();

        if (oldDelta >= 3) {
//            System.out.println(oldDelta + " " + "set " + key);
            value.set(ind, true);
        }

        TIntList dstList = data.get(ind);
        if (!filled.get(ind)) {
            join(indKeyList.get(ind), dstList);
            filled.set(ind, true);
        }

        if (value.get(ind) && !send.get(ind)) {
            for (int i = 0; i < dstList.size(); i++) {
                int dst = dstList.get(i);
                int dstInd = keyIndMap.get(dst);
                deltaValue.get(dstInd).accumulateAndGet(1, Integer::sum);
            }
            send.set(ind, true);
        }

    }

    public void join(int key, TIntList dstList) {
        TIntList list = bucket.get(key);
        list.forEach(dst->{
            dstList.add(dst);
            addKey(dst, false, 0);
            return true;
        });
    }

    private synchronized boolean addKey(int key, boolean initValue, int initDelta) {
        if (keyIndMap.containsKey(key))
            return false;
        keyIndMap.put(key, size);
        indKeyList.add(key);
        value.add(initValue);
        deltaValue.add(new AtomicInteger(initDelta));
        data.add(new TIntArrayList());
        filled.add(false);
        send.add(false);
        size++;
        return true;
    }


    public synchronized int getSize() {
        return size;
    }

    @Override
    public void iterate(MyVisitorImpl visitor) {

    }

    @Override
    public void iterateTuple(QueryVisitor queryVisitor) {

    }

    @Override
    public double getValue(int localInd) {
        return 0;
    }

    @Override
    public double getDelta(int localInd) {
        return 0;
    }

    @Override
    public double accumulateValue() {
        return 0;
    }

    @Override
    public double accumulateDelta() {
        return 0;
    }

    @Override
    public MyVisitorImpl getInitVisitor() {
        return null;
    }



    VisitorImpl initVisitor = new VisitorImpl() {
        @Override
        public int getEpochId() {
            return 0;
        }

        @Override
        public int getRuleId() {
            return 0;
        }

        @Override
        public boolean visit(int a1) {
            addKey(a1, true, 0);
            return true;
        }
    };

    VisitorImpl edgeVisitor = new VisitorImpl() {
        int src;

        @Override
        public int getEpochId() {
            return 0;
        }

        @Override
        public int getRuleId() {
            return 0;
        }

        TIntArrayList dstList;
        @Override
        public boolean visit_0(int key) {
            dstList = new TIntArrayList();
            bucket.put(key, dstList);
            return true;
        }

        @Override
        public boolean visit(int dst ) {
            dstList.add(dst);
            return true;
        }
    };
}