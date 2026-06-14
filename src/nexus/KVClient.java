package nexus;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

public class KVClient {
    public static void main(String[] args) throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost", 1099);
        KVStore store = (KVStore) registry.lookup("KVStore");

        System.out.println("متصل بالـ KV Node. الأوامر: PUT key value | GET key | DEL key | SIZE | EXIT");
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split("\\s+", 3);
            String cmd = parts[0].toUpperCase();

            switch (cmd) {
                case "PUT" -> {
                    if (parts.length < 3) { System.out.println("الاستخدام: PUT key value"); break; }
                    String old = store.put(parts[1], parts[2]);
                    System.out.println("تم. القيمة القديمة: " + old);
                }
                case "GET" -> {
                    if (parts.length < 2) { System.out.println("الاستخدام: GET key"); break; }
                    System.out.println(store.get(parts[1]));
                }
                case "DEL" -> {
                    if (parts.length < 2) { System.out.println("الاستخدام: DEL key"); break; }
                    System.out.println(store.delete(parts[1]) ? "تم الحذف" : "المفتاح غير موجود");
                }
                case "SIZE" -> System.out.println("عدد المفاتيح: " + store.size());
                case "EXIT" -> { System.out.println("مع السلامة"); return; }
                default -> System.out.println("أمر غير معروف");
            }
        }
    }
}