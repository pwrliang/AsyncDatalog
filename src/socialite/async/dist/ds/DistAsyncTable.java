package socialite.async.dist.ds;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.atomic.MyAtomicDouble;
import socialite.resource.DistTableSliceMap;
import socialite.util.SociaLiteException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

//this is array table
//for dynamic added and pair-key algorithms, hash table is needed
public class DistAsyncTable extends BaseDistAsyncTable {
    public static final double IDENTITY_ELEMENT = 0;
    private final int workerNum;
    private TIntIntHashMap keyIndMap; //T<Key>IntHashMap in template
    private List<MyAtomicDouble> deltaList;
    private TDoubleList valueList;
    private List<TIntList> dataList;
    private volatile int size;
    private TIntList extra;
    private AtomicIntegerArray messageTableSelector;
    private MessageTable[][] messageTableList;

    private int myWorkerId;

    public DistAsyncTable(int workerNum, int myWorkerId, int initSize, int initBufferTableSize) {
        this.workerNum = workerNum;
        this.myWorkerId = myWorkerId;

        keyIndMap = new TIntIntHashMap(initSize);
        deltaList = new ArrayList<>(initSize);
        valueList = new TDoubleArrayList(initSize);
        dataList = new ArrayList<>(initSize);
        extra = new TIntArrayList(initSize);

        messageTableSelector = new AtomicIntegerArray(workerNum);
        messageTableList = new MessageTable[workerNum][2];
        for (int wid = 0; wid < workerNum; wid++) {
            if (wid == myWorkerId) continue;//for worker i, it have 0,1,...,i-1,null,i+1,...n-1 buffer table
            messageTableList[wid][0] = new MessageTable(initBufferTableSize);
            messageTableList[wid][1] = new MessageTable(initBufferTableSize);
        }
    }

    public void updateLockFree(int localInd) {
        double oldDelta = deltaList.get(localInd).getAndSet(IDENTITY_ELEMENT);
        valueList.set(localInd, Double.sum(valueList.get(localInd), oldDelta));//Type.sum/max/min


        TIntList data = dataList.get(localInd);
        double newDelta = eval(localInd, 0, oldDelta);

        for (int i = 0; i < data.size(); i++) {
            int dst = data.get(i);
            int belongToWorkerId = getWorkerId(dst, workerNum);
            if (belongToWorkerId == myWorkerId) {
                int dstLocalInd = keyIndMap.get(dst);
                deltaList.get(dstLocalInd).accumulateAndGet(newDelta, Double::sum);
            } else {
                MessageTable messageTable = messageTableList[belongToWorkerId][messageTableSelector.get(belongToWorkerId)];
                messageTable.apply(dst, newDelta);
            }
        }
    }

    /**
     * 将远程Worker发来的Buffer累积到本地DistAsyncTable
     *
     * @param messageTable 远程BufferTable
     */
    @Override
    public void applyBuffer(MessageTable messageTable) {
        messageTable.getKeyDeltaMap().forEach((key, delta) -> {
            if (!keyIndMap.containsKey(key))
                throw new SociaLiteException((myWorkerId + 1) + "have not key " + key);
            int localInd = keyIndMap.get(key);
            deltaList.get(localInd).accumulateAndGet(delta.get(), Double::sum);
        });
    }

    int tableId;

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    // should same as InitCarrier.getWorkerId
    public int getWorkerId(int key, int workerNum) {
        return sliceMap.machineIndexFor(tableId, key);
    }

    @Override
    public Object getValue() {
        return valueList;
    }

    @Override
    public Double accumulateValue() {
        double sum = 0;
        for (int i = 0; i < size; i++) {
            sum += valueList.get(i);
        }
        return sum;
    }

    private double eval(int ind, int weight, double oldDelta) {
        return oldDelta * 0.8 / extra.get(ind);
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void iterate(MyVisitorImpl visitor) {
        keyIndMap.forEachEntry((key, ind) -> {
                    visitor.visit(key, valueList.get(ind), deltaList.get(ind).doubleValue());
                    return true;
                }
        );
    }

    //for dynamically algorithms
    public synchronized void addEntryDynamically(int key, int value, int delta, int extra) {

    }

    @Override
    public AtomicIntegerArray getMessageTableSelector() {
        return messageTableSelector;
    }

    @Override
    public MessageTable[] getMessageAndBackup(int workerId) {
        return messageTableList[workerId];
    }

    private static final Log L = LogFactory.getLog(DistAsyncTable.class);

    public void display() {
        L.info("ind\tkey\tvalue\tDvalue\tadjacent\textra");
        keyIndMap.forEachEntry((key, ind) -> {
            double value = valueList.get(ind);
            double dValue = deltaList.get(ind).doubleValue();
            TIntList adjacents = dataList.get(ind);

            L.info(String.format("%d\t\t%d\t%f\t%f\t%s\t%d", ind, key, value, dValue, Arrays.toString(adjacents.toArray()), extra.get(ind)));
            return true;
        });
    }

    DistTableSliceMap sliceMap;

    public void setSliceMap(DistTableSliceMap sliceMap) {
        this.sliceMap = sliceMap;
    }

    public MyVisitorImpl getMiddleVisitor() {
        return new MyVisitorImpl() {
            TIntArrayList adjacents;

            //"Middle(int Key:0..875713, double initD, int degree, (int adj))."
            @Override
            public boolean visit_0_1_2(int a1, double a2, int a3) {
                keyIndMap.put(a1, size++);
                valueList.add(IDENTITY_ELEMENT);
                deltaList.add(new MyAtomicDouble(a2));
                extra.add(a3);
                dataList.add(adjacents);
                adjacents = new TIntArrayList();
                return true;
            }

            @Override
            public boolean visit(int a1) {
                adjacents.add(a1);
                return true;
            }
        };
    }

}
