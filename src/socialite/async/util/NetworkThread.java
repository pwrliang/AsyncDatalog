package socialite.async.util;

public class NetworkThread {
    static NetworkThread networkThread;

    private NetworkThread() {

    }

    public synchronized static NetworkThread getInst() {
        if (networkThread == null)
            networkThread = new NetworkThread();
        return networkThread;
    }

//    public void sendTo(int workerId, )
}
