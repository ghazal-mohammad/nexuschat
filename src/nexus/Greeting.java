package nexus;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Greeting extends Remote {
    String sayHello(String name) throws RemoteException;
}