package socialite.async.codegen;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import socialite.async.analysis.MyVisitorImpl;
import socialite.async.atomic.MyAtomicDouble;
import socialite.async.dist.ds.MessageTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class DistAsyncTableSSSP extends BaseAsyncTable {
    private TIntIntHashMap keyIndMap;
    private TIntIntHashMap indKeyMap;//dynamic only
    private TIntObjectHashMap<TIntArrayList> col1AdjacenciesMap;//dynamic only
    private TIntObjectHashMap<TIntArrayList> col1WeightsMap;//dynamic only
    ///////////UNIVERSE//////////
    private TDoubleList valueList;
    private List<MyAtomicDouble> deltaList;
    private AtomicIntegerArray messageTableSelector;
    private MessageTable[][] messageTableList;
    private volatile int size;
    ///////////UNIVERSE//////////
    //private TIntList extra; PageRank
    ///////////CLUSTER INFO//////
    Map<Integer, Integer> myIdxWorkerIdMap;
    private final int workerNum;
    private final int myWorkerId;
    ///////////CLUSTER INFO//////
    public static final int IDENTITY_ELEMENT = 0;

    public DistAsyncTableSSSP(int workerNum, int myWorkerId, int initSize, int initMessageTableSize, Map<Integer, Integer> myIdxWorkerIdMap) {
        this.workerNum = workerNum;
        this.myWorkerId = myWorkerId;

    }

    @Override
    public void updateLockFree(int localInd) {
        super.updateLockFree(localInd);
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public void iterate(MyVisitorImpl visitor) {

    }

    @Override
    public Object accumulateValue() {
        return null;
    }

    @Override
    public Object accumulateDelta() {
        return null;
    }

    @Override
    public MyVisitorImpl getInitVisitor() {
        return null;
    }

    @Override
    public MyVisitorImpl getEdgeVisitor() {
        return new MyVisitorImpl() {
            TIntArrayList adjacencies;
            TIntArrayList weights;
            @Override
            public boolean visit_0(int key) {
                adjacencies = new TIntArrayList();
                col1AdjacenciesMap.put(key,adjacencies);
                weights = new TIntArrayList();
                col1WeightsMap.put(key,weights);
                return true;
            }

            @Override
            public boolean visit(int col2, int weight) {

                return true;
            }
        };
    }
}
