package nexus;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface KVStore extends Remote {
    String put(String key, String value) throws RemoteException;
    String get(String key) throws RemoteException;
    boolean delete(String key) throws RemoteException;
    int size() throws RemoteException;
}