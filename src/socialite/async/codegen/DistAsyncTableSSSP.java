package socialite.async.codegen;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.atomic.MyAtomicInteger;
import socialite.async.dist.ds.BaseDistAsyncTable;
import socialite.async.dist.ds.MessageTable;
import socialite.resource.DistTableSliceMap;
import socialite.util.SociaLiteException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class DistAsyncTableSSSP extends BaseDistAsyncTable {
    private static final Log L = LogFactory.getLog(DistAsyncTableSSSP.class);
    private TIntIntHashMap keyIndMap;
    private TIntIntHashMap indKeyMap; //using in iterate method
    private TIntObjectHashMap<TIntArrayList> srcDstListMap;
    private TIntObjectHashMap<TIntArrayList> srcWeightListMap;
    private TIntArrayList valueList;
    private List<MyAtomicInteger> deltaList;
    private List<TIntArrayList> adjacenciesList; //除了key为int类型的装ind，该字段装邻接点数据
    private List<TIntArrayList> weightsList;
    ////////////////////////
    private final int workerNum;
    private int myWorkerId;
    private AtomicIntegerArray messageTableSelector;
    private MessageTable[][] messageTableList;
    private final DistTableSliceMap sliceMap;
    private final int indexForTableId;
    /////////////////////////////
    public static final int IDENTITY_ELEMENT = Integer.MAX_VALUE;
    private int size;

    public DistAsyncTableSSSP(int workerNum, int myWorkerId, DistTableSliceMap sliceMap, int indexForTableId, int initSize, int initBufferTableSize) {
        this.workerNum = workerNum;
        this.myWorkerId = myWorkerId;
        this.sliceMap = sliceMap;
        this.indexForTableId = indexForTableId;

        keyIndMap = new TIntIntHashMap(initSize);
        indKeyMap = new TIntIntHashMap(initSize); //using in iterate method

        srcDstListMap = new TIntObjectHashMap<>(initSize);
        srcWeightListMap = new TIntObjectHashMap<>(initSize);

        valueList = new TIntArrayList(initSize);
        deltaList = new ArrayList<>(initSize);
        adjacenciesList = new ArrayList<>(initSize);
        weightsList = new ArrayList<>(initSize);

        messageTableSelector = new AtomicIntegerArray(workerNum);
        messageTableList = new MessageTable[workerNum][2];
        for (int wid = 0; wid < workerNum; wid++) {
            if (wid == myWorkerId) continue;//for worker i, it have 0,1,...,i-1,null,i+1,...n-1 buffer table
            messageTableList[wid][0] = new MessageTable(initBufferTableSize);
            messageTableList[wid][1] = new MessageTable(initBufferTableSize);
        }
    }

    @Override
    public void updateLockFree(int localInd) {
        int oldDelta = deltaList.get(localInd).getAndSet(IDENTITY_ELEMENT);
        if (oldDelta == IDENTITY_ELEMENT)
            return;

        //keyType: int aggrType: dmin weightType:int extraType:
        int accumulatedValue = Integer.min(valueList.get(localInd), oldDelta);
        if (accumulatedValue >= valueList.get(localInd)) return;//更新value
        valueList.set(localInd, accumulatedValue);
        TIntArrayList weights = weightsList.get(localInd);

        TIntArrayList adjacencies = adjacenciesList.get(localInd);
        if (adjacencies != null) {
            for (int i = 0; i < adjacencies.size(); i++) {
                int newDelta = eval(oldDelta, weights.get(i));
                int adjacentLocalInd;
                int adjacency = adjacencies.get(i);
                int belongToWorkerId = sliceMap.machineIndexFor(indexForTableId, adjacency);
                if (belongToWorkerId == myWorkerId) {
                    if (keyIndMap.contains(adjacency))
                        adjacentLocalInd = keyIndMap.get(adjacency);
                    else {
                        adjacentLocalInd = addEntry(adjacency, IDENTITY_ELEMENT, IDENTITY_ELEMENT);
                    }
                    deltaList.get(adjacentLocalInd).accumulateAndGet(newDelta, Math::min);
                } else {
                    MessageTable messageTable = messageTableList[belongToWorkerId][messageTableSelector.get(belongToWorkerId)];
                    messageTable.apply(adjacency, newDelta);
                }
            }
        }
    }

    @Override
    public void applyBuffer(MessageTableBase messageTable) {
//        messageTable.getKeyDeltaMap().forEach((key, delta) -> {
//            if (!keyIndMap.containsKey(key))
//                throw new SociaLiteException((myWorkerId + 1) + "have not key " + key);
//            int localInd = keyIndMap.get(key);
//            deltaList.get(localInd).accumulateAndGet(delta.get(), Integer::sum);
//        });
    }

    private int eval(int oldDelta, int weight) {
        return (int) (oldDelta + weight);
    }

    private synchronized int addEntry(int key, int value, int delta) {
        keyIndMap.put(key, size);
        indKeyMap.put(size, key);
        valueList.add(value);
        deltaList.add(new MyAtomicInteger(delta));
        adjacenciesList.add(join(key));
        weightsList.add(srcWeightListMap.get(key));
        return size++;
    }

    private synchronized TIntArrayList join(int key) {
        return srcDstListMap.get(key);
    }

    @Override
    public MyVisitorImpl getInitVisitor() {
        return new MyVisitorImpl() {
            @Override
            public boolean visit(int key, int delta) {
                addEntry(key, IDENTITY_ELEMENT, delta);
                return true;
            }
        };
    }

    @Override
    public MyVisitorImpl getEdgeVisitor() {
        return new MyVisitorImpl() {
            TIntArrayList dstList;
            TIntArrayList weightList;

            @Override
            public boolean visit_0(int key) {
                dstList = new TIntArrayList();
                srcDstListMap.put(key, dstList);
                weightList = new TIntArrayList();
                srcWeightListMap.put(key, weightList);
                return true;
            }

            @Override
            public boolean visit(int dst, int weight) {
                dstList.add(dst);
                weightList.add(weight);
                return true;
            }
        };
    }

    @Override
    public MessageTable[] getMessageTables(int workerId) {
        return new MessageTable[0];
    }

    @Override
    public AtomicIntegerArray getMessageTableSelector() {
        return null;
    }



    @Override
    public Integer accumulateValue() {
        int sum = 0;
        for (int i = 0; i < size; i++) {
            int value = valueList.get(i);
            if (value != IDENTITY_ELEMENT)
                sum += valueList.get(i);
        }
        return sum;
    }

    @Override
    public Integer accumulateDelta() {
        int sum = 0;
        for (int i = 0; i < size; i++) {
            int delta = deltaList.get(i).intValue();
            if (delta != IDENTITY_ELEMENT)
                sum += delta;
        }
        return sum;
    }

    @Override
    public void iterate(MyVisitorImpl visitor) {
        for (int i = 0; i < size; i++) {
            if (!visitor.visit(indKeyMap.get(i), valueList.get(i), deltaList.get(i).intValue()))
                break;
        }
    }

    @Override
    public int getSize() {
        return size;
    }
}
