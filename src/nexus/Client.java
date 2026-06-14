package nexus;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {
    public static void main(String[] args) throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost", 1099);
        Greeting greeting = (Greeting) registry.lookup("GreetingService");
        System.out.println(greeting.sayHello("المهندس"));
    }
}