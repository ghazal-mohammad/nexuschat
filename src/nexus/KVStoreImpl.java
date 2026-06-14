package nexus;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVStoreImpl extends UnicastRemoteObject implements KVStore {
    private final Map<String, String> data = new ConcurrentHashMap<>();

    protected KVStoreImpl() throws RemoteException { super(); }

    @Override
    public String put(String key, String value) throws RemoteException {
        String old = data.put(key, value);
        System.out.println("[PUT] " + key + " = " + value);
        return old;
    }

    @Override
    public String get(String key) throws RemoteException {
        String value = data.get(key);
        System.out.println("[GET] " + key + " -> " + value);
        return value;
    }

    @Override
    public boolean delete(String key) throws RemoteException {
        boolean removed = data.remove(key) != null;
        System.out.println("[DEL] " + key + (removed ? " (removed)" : " (not found)"));
        return removed;
    }

    @Override
    public int size() throws RemoteException {
        return data.size();
    }
}