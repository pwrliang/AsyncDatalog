package socialite.async.dist;

import socialite.async.AsyncConfig;

import java.util.LinkedHashMap;
import java.util.Map;

public class Payload {
    private Map<Integer, Integer> myIdxWorkerIdMap;
    private LinkedHashMap<String, byte[]> byteCodes;
    private String edgeTableName;
    private AsyncConfig asyncConfig;
    private Payload(){}
    public Payload(AsyncConfig asyncConfig,  Map<Integer, Integer> myIdxWorkerIdMap, LinkedHashMap<String, byte[]> byteCodes, String edgeTableName){
        this.asyncConfig = asyncConfig;
        this.myIdxWorkerIdMap = myIdxWorkerIdMap;
        this.byteCodes=byteCodes;
        this.edgeTableName=edgeTableName;
    }

    public Map<Integer, Integer> getMyIdxWorkerIdMap() {
        return myIdxWorkerIdMap;
    }

    public LinkedHashMap<String, byte[]> getByteCodes() {
        return byteCodes;
    }

    public String getEdgeTableName() {
        return edgeTableName;
    }

    public AsyncConfig getAsyncConfig() {
        return asyncConfig;
    }
}
