package socialite.async.dist;

import java.util.LinkedHashMap;

public class Payload {
    LinkedHashMap<String, byte[]> byteCodes;
    String edgeTableName;
    private Payload(){}
    public Payload(LinkedHashMap<String, byte[]> byteCodes, String edgeTableName){
        this.byteCodes=byteCodes;
        this.edgeTableName=edgeTableName;
    }

    public LinkedHashMap<String, byte[]> getByteCodes() {
        return byteCodes;
    }

    public String getEdgeTableName() {
        return edgeTableName;
    }
}
