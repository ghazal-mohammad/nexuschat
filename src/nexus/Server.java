package nexus;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Server {
    public static void main(String[] args) throws Exception {
        Registry registry = LocateRegistry.createRegistry(1099);
        registry.rebind("GreetingService", new GreetingImpl());
        System.out.println("الخادم جاهز — الخدمة مسجّلة على المنفذ 1099");
    }
}