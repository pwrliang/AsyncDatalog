package socialite.async.codegen;


import socialite.async.analysis.MyVisitorImpl;
import socialite.async.dist.ds.MessageTable;
import socialite.async.util.SerializeTool;
import socialite.resource.DistTableSliceMap;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class BaseDistAsyncTable extends BaseAsyncTable {
    private AtomicIntegerArray messageTableSelector;
    private MessageTableBase[][] messageTableList;
    int workerNum;
    int myWorkerId;
    final DistTableSliceMap sliceMap;
    final int indexForTableId;
    final int base;
    final int messageTableUpdateThreshold;

    public BaseDistAsyncTable(int workerNum, int myWorkerId, DistTableSliceMap sliceMap, int indexForTableId,  int base, int messageTableUpdateThreshold,int initBufferTableSize) {
        this.workerNum = workerNum;
        this.myWorkerId = myWorkerId;
        this.sliceMap = sliceMap;
        this.indexForTableId = indexForTableId;
        this.base = base;
        this.messageTableUpdateThreshold = messageTableUpdateThreshold;

        messageTableSelector = new AtomicIntegerArray(workerNum);
        messageTableList = new MessageTable[workerNum][2];
        for (int wid = 0; wid < workerNum; wid++) {
            if (wid == myWorkerId) continue;//for worker i, it have 0,1,...,i-1,null,i+1,...n-1 buffer table
            messageTableList[wid][0] = new MessageTable(initBufferTableSize);
            messageTableList[wid][1] = new MessageTable(initBufferTableSize);
        }

        this.workerNum = workerNum;
        this.myWorkerId = myWorkerId;
    }

    public MessageTableBase[] getMessageTables(int workerId) {
        return messageTableList[workerId];
    }

    public AtomicIntegerArray getMessageTableSelector() {
        return messageTableSelector;
    }

    public MessageTableBase getWritableMessageTable(int workerId) {
        return messageTableList[workerId][messageTableSelector.get(workerId)];
    }

    public byte[] getSendableMessageTableBytes(int sendToWorkerId, SerializeTool serializeTool) throws InterruptedException {
        int writingTableInd = messageTableSelector.get(sendToWorkerId);//获取计算线程正在写入的表序号
        MessageTableBase[] messageTableAndBackup = messageTableList[sendToWorkerId];
        MessageTableBase sendableMessageTable = messageTableAndBackup[writingTableInd];
        while (sendableMessageTable.getUpdateTimes() < messageTableUpdateThreshold)
            Thread.sleep(100);
        messageTableSelector.set(sendToWorkerId, writingTableInd == 0 ? 1 : 0);
        byte[] data = serializeTool.toBytes(sendableMessageTable);
        sendableMessageTable.resetDelta();
        return data;
    }

    public abstract void applyBuffer(MessageTableBase messageTable);

    public abstract Object accumulateValue();

    public MyVisitorImpl getEdgeVisitor() {
        throw new NotImplementedException();
    }

    public MyVisitorImpl getInitVisitor() {
        throw new NotImplementedException();
    }
}
