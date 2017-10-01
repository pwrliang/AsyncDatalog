package socialite.async.dist.master;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.ArrayList;
import java.util.List;

//dynamic generate required
public class InitCarrier {
    private transient TIntIntHashMap keyIndMap;
    private TIntList keyList;
    private TDoubleList valueList;
    private TDoubleList deltaList;
    private List<TIntList> adjacentList;
    private TIntList extraList;
    private int size;
    private int workerId;

    private InitCarrier() {
    }//for kryo

    public InitCarrier(int initSize, int belongToWorkerId) {
        keyIndMap = new TIntIntHashMap(initSize);
        keyList = new TIntArrayList(initSize);
        valueList = new TDoubleArrayList(initSize);
        deltaList = new TDoubleArrayList(initSize);
        adjacentList = new ArrayList<>(initSize);
        extraList = new TIntArrayList(initSize);
        workerId = belongToWorkerId;
    }

    public static int getWorkerId(Object key, int workerNum) {
        return key.hashCode() % workerNum;
    }

    public synchronized void addEntry(int key, int value, double delta, int adjacent, int extra) {
        TIntList adjacents;
        if (keyIndMap.containsKey(key)) {
            int ind = keyIndMap.get(key);
            adjacents = adjacentList.get(ind);
            adjacents.add(adjacent);
        } else {
            keyIndMap.put(key, size);
            keyList.add(key);
            valueList.add(value);
            deltaList.add(delta);
            adjacents = new TIntArrayList();
            adjacents.add(adjacent);
            adjacentList.add(adjacents);
            extraList.add(extra);
            size++;
        }
    }

    public int getSize() {
        return size;
    }

    public int getWorkerId() {
        return workerId;
    }

    public TIntList getKeyList() {
        return keyList;
    }

    public TDoubleList getValueList() {
        return valueList;
    }

    public TDoubleList getDeltaList() {
        return deltaList;
    }

    public List<TIntList> getAdjacentList() {
        return adjacentList;
    }

    public TIntList getExtraList() {
        return extraList;
    }
}
