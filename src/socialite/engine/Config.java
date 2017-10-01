package socialite.engine;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.dist.PortMap;
import socialite.util.Assert;
import socialite.util.SociaLiteException;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Set;

enum EngineType {
    SEQ, PARALLEL, DIST, CLIENT
}

public class Config {
    public static final Log L = LogFactory.getLog(Config.class);
    private static Config inst;

    public static Config getInst() {
        return inst;
    }

    public static Config seq() {
        inst = new Config(EngineType.SEQ);
        return inst;
    }

    public static Config par() {
        inst = new Config(EngineType.PARALLEL);
        return inst;
    }

    public static Config par(int n) {
        inst = new Config(EngineType.PARALLEL, n);
        return inst;
    }

    public static Config client(String master, int port) {
        inst = new Config(EngineType.CLIENT);
        inst.portMap = new PortMap(master, port);
        return inst;
    }

    public static Config client() {
        inst = new Config(EngineType.CLIENT);
        inst.portMap = PortMap.get();
        return inst;
    }

    public static Config dist() {
        inst = new Config(EngineType.DIST);
        inst.portMap = PortMap.get();
        return inst;
    }

    public static Config dist(int cpuNum) {
        inst = new Config(EngineType.DIST, cpuNum);
        inst.portMap = PortMap.get();
        return inst;
    }

    static Set<String> workerNames = new LinkedHashSet<String>();
    static int workerNodeNum = -1;

    public static synchronized Set<String> getWorkers() {
        if (workerNames.size() != 0) {
            return workerNames;
        }

        try {
            FileInputStream fis = new FileInputStream("conf/slaves");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                line = line.trim();
                if (line.length() == 0) continue;
                if (line.startsWith("#")) continue;
                workerNames.add(line);
            }
            return workerNames;
        } catch (FileNotFoundException e) {
            L.warn("Cannot find conf/slaves.");
            return workerNames;
        } catch (IOException e) {
            L.warn("Cannot parse conf/slaves file.");
            return workerNames;
        }
    }

    public static int getWorkerNodeNum() {
        if (workerNodeNum == -1) workerNodeNum = getWorkers().size();
        return workerNodeNum;
    }

    public static int systemWorkerNum = -1;

    static {
        String p = System.getProperty("socialite.worker.num");
        if (p != null) {
            systemWorkerNum = Integer.parseInt(p);
        } else {
            systemWorkerNum = -1;
        }
    }

    int workerThreadNum = 1;
    boolean verbose = false;
    boolean cleanup = false;

    EngineType engineType = null;
    boolean isClient = false;
    boolean isDistributed = false;
    PortMap portMap = null;

    boolean errorRecovery = false;
    TObjectIntHashMap<String> debugInfo;

    void setupDebugInfo() {
        debugInfo = new TObjectIntHashMap<String>();
        debugInfo.put("GenerateTable", (byte) 1);
        debugInfo.put("GenerateVisitor", (byte) 1);
        debugInfo.put("GenerateEval", (byte) 1);
        debugInfo.put("DeltaStepOpt", (byte) 1);
        debugInfo.put("DijkstraOpt", (byte) 1);
    }

    public Config(EngineType type) {
        engineType = type;
        setWorkerNum(type, -1);
        setupDebugInfo();
    }

    public Config(EngineType type, int _cpuNum) {
        engineType = type;
        setWorkerNum(type, _cpuNum);
        setupDebugInfo();
    }

    public void setDebugOpt(String debugOpt, boolean value) {
        if (!debugInfo.containsKey(debugOpt)) {
            throw new SociaLiteException("Unexpected Debug Option:" + debugOpt);
        }
        if (value) debugInfo.put(debugOpt, 1);
        else debugInfo.put(debugOpt, 0);
    }

    public boolean getDebugOpt(String debugOpt) {
        int value = debugInfo.get(debugOpt);
        if (value == 1) return true;
        else return false;
    }

    public int getWorkerThreadNum() {
        return workerThreadNum;
    }

    public void setCleanup(boolean _cleanup) {
        cleanup = _cleanup;
    }

    public boolean cleanup() {
        return cleanup;
    }

    public boolean errorRecovery() {
        return errorRecovery;
    }

    public void setErrorRecovery(boolean recover) {
        errorRecovery = recover;
    }

    void setWorkerNum(EngineType type, int _cpuNum) {
        if (_cpuNum <= 0) {
            if (type == EngineType.DIST) {
                if (systemWorkerNum > 0) {
                    workerThreadNum = systemWorkerNum;
                } else {
                    Assert.impossible("systemWorkerNum<0 and cpuNum<0");
                    //System.out.println(" WARN uncomment Assert.impossible in Config::setWorkerThreadNum");
                }
            } else if (type == EngineType.PARALLEL) {
                workerThreadNum = Runtime.getRuntime().availableProcessors();
            } else {
                workerThreadNum = 1;
            }
        } else {
            workerThreadNum = _cpuNum;
        }
    }

    public int sliceNum() {
        if (workerThreadNum == 1) return 1;
        else {
            /*if (isDistributed()) return workerThreadNum *8;
            else return workerThreadNum *8;*/
            return workerThreadNum * 16;
        }
    }

    public int virtualSliceNum() {
        if (workerThreadNum == 1) return 1;
        else {
            /*if (isDistributed()) return workerThreadNum *8;
            else return workerThreadNum *8;*/
            return workerThreadNum * 16;
        }
        //else return workerThreadNum*2;
    }

    public void setVerbose() {
        verbose = true;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isClient() {
        return engineType == EngineType.CLIENT;
    }

    public boolean isSequential() {
        return workerThreadNum == 1 && !isDistributed();
    }

    public boolean isParallel() {
        return workerThreadNum >= 2 || isDistributed();
    }

    public boolean isDistributed() {
        return engineType == EngineType.DIST;
    }

    public int minSliceSize() {
        return 1;
    }

    public PortMap portMap() {
        assert (portMap != null);
        return portMap;
    }
}
