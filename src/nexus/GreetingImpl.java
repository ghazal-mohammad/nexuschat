package nexus;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class GreetingImpl extends UnicastRemoteObject implements Greeting {
    protected GreetingImpl() throws RemoteException { super(); }

    @Override
    public String sayHello(String name) throws RemoteException {
        return "Hello " + name + " — أول نداء عن بُعد اشتغل!";
    }
}