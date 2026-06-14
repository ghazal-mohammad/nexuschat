package nexus;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class KVNode {
    public static void main(String[] args) throws Exception {
        int port = 1099;
        Registry registry = LocateRegistry.createRegistry(port);
        registry.rebind("KVStore", new KVStoreImpl());
        System.out.println("KV Node جاهزة — KVStore مسجّلة على المنفذ " + port);
    }
}