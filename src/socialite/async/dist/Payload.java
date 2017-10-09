package socialite.async.dist;

import socialite.async.AsyncConfig;

import java.util.LinkedHashMap;

public class Payload {
    private LinkedHashMap<String, byte[]> byteCodes;
    private String edgeTableName;
    private AsyncConfig asyncConfig;
    private Payload(){}
    public Payload(AsyncConfig asyncConfig, LinkedHashMap<String, byte[]> byteCodes, String edgeTableName){
        this.byteCodes=byteCodes;
        this.edgeTableName=edgeTableName;
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
