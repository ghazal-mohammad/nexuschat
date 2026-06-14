package nexus;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.Executors;

public class RaftCluster {

    public static void main(String[] args) throws Exception {
        RaftNodeImpl n1 = startNode(1, 1099, List.of(1100, 1101));
        RaftNodeImpl n2 = startNode(2, 1100, List.of(1099, 1101));
        RaftNodeImpl n3 = startNode(3, 1101, List.of(1099, 1100));

        Map<Integer, RaftNodeImpl> nodes = new LinkedHashMap<>();
        nodes.put(1, n1); nodes.put(2, n2); nodes.put(3, n3);

        GossipProtocol gossip = new GossipProtocol(1, nodes);
        gossip.start();

        Bulkhead chatBulkhead    = new Bulkhead("chat",    10, 50);
        Bulkhead kvBulkhead      = new Bulkhead("kv",       5, 20);
        Bulkhead metricsBulkhead = new Bulkhead("metrics",  2, 10);

        LoadBalancer lb = new LoadBalancer(nodes);
        MetricsCollector metrics = new MetricsCollector();
        lb.setMetrics(metrics);

        // بورت واحد للوحة التحكم والدردشة معاً (يقرأ متغير البيئة PORT عند النشر)
        int port = resolvePort();
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
        http.setExecutor(Executors.newCachedThreadPool());

        Dashboard dashboard = new Dashboard(nodes, lb, gossip, chatBulkhead, kvBulkhead, metrics);
        dashboard.registerOn(http);

        ChatServer chatServer = new ChatServer();
        chatServer.registerOn(http);

        http.start();
        chatServer.start();

        System.out.println("=========================================================");
        System.out.println("  NexusChat — Distributed Systems Demo");
        System.out.println("  Dashboard + Chat: http://localhost:" + port);
        System.out.println("=========================================================");
        System.out.println("  put k v       — اكتب مفتاح");
        System.out.println("  read k        — اقرأ مفتاح");
        System.out.println("  loadtest N    — اختبار حمل");
        System.out.println("  algo RR|WRR|LC|LRT|SS|CH|P2|JIQ  — بدّل خوارزمية LB");
        System.out.println("  weight id N   — وزن عقدة للـ Weighted");
        System.out.println("  retry         — اختبار Retry+Backoff");
        System.out.println("  bulkhead      — احصائيات Bulkheads");
        System.out.println("  gossip        — حالة Gossip Protocol");
        System.out.println("  vc            — عرض Vector Clocks");
        System.out.println("  chat          — حالة الدردشة");
        System.out.println("  fail id       — فشل عقدة (Circuit Breaker)");
        System.out.println("  recover id    — استعادة عقدة");
        System.out.println("  N             — اقتل عقدة (crash)");
        System.out.println("  r N           — احيِ عقدة");
        System.out.println("  status        — حالة الكلستر\n");

        new Thread(n1::run, "node-1").start();
        new Thread(n2::run, "node-2").start();
        new Thread(n3::run, "node-3").start();

        VectorClock[] vcs = {
            new VectorClock(0, 3),
            new VectorClock(1, 3),
            new VectorClock(2, 3)
        };
        RetryPolicy retryPolicy = RetryPolicy.defaultPolicy(4);

        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.isEmpty()) continue;
            String low = line.toLowerCase();
            try {
                if (low.equals("status") || low.equals("log")) {
                    for (RaftNodeImpl n : nodes.values()) System.out.println("   " + n.dumpState());
                    System.out.println(gossip.summary());

                } else if (low.startsWith("put ") || low.startsWith("del ")) {
                    vcs[0].tick();
                    System.out.println(">>> VC: " + vcs[0]);
                    System.out.println(">>> " + lb.write(newId(), line));

                } else if (low.startsWith("read ")) {
                    String[] parts = line.substring(5).trim().split("\\s+");
                    String key = parts[0];
                    String clientId = parts.length > 1 ? parts[1] : null;
                    System.out.println(">>> " + lb.read(key, clientId != null ? clientId : key));

                } else if (low.startsWith("loadtest ")) {
                    int n = Integer.parseInt(line.substring(9).trim());
                    kvBulkhead.submit(() -> { lb.loadTest(n, "color"); return null; });

                } else if (low.startsWith("algo ")) {
                    String name = line.substring(5).trim().toUpperCase();
                    LoadBalancer.Algorithm alg;
                    switch (name) {
                        case "RR":  alg = LoadBalancer.Algorithm.ROUND_ROBIN;          break;
                        case "WRR": alg = LoadBalancer.Algorithm.WEIGHTED_ROUND_ROBIN; break;
                        case "LC":  alg = LoadBalancer.Algorithm.LEAST_CONNECTIONS;    break;
                        case "LRT": alg = LoadBalancer.Algorithm.LEAST_RESPONSE_TIME;  break;
                        case "SS":  alg = LoadBalancer.Algorithm.STICKY_SESSION;       break;
                        case "CH":  alg = LoadBalancer.Algorithm.CONSISTENT_HASH;      break;
                        case "P2":  alg = LoadBalancer.Algorithm.POWER_OF_TWO;         break;
                        case "JIQ": alg = LoadBalancer.Algorithm.JOIN_IDLE_QUEUE;      break;
                        default:    alg = LoadBalancer.Algorithm.valueOf(name);
                    }
                    lb.setAlgorithm(alg);

                } else if (low.startsWith("weight ")) {
                    String[] p = line.split("\\s+");
                    lb.setWeight(Integer.parseInt(p[1]), Integer.parseInt(p[2]));

                } else if (low.equals("retry")) {
                    System.out.println(">>> اختبار Retry+Backoff...");
                    nodes.get(1).setFailing(true);
                    String result = retryPolicy.executeOrNull("read-test",
                        () -> { String v = nodes.get(1).get("color");
                                if (v == null) throw new RuntimeException("Node1 فاشلة");
                                return v; });
                    nodes.get(1).setFailing(false);
                    System.out.println(">>> نتيجة: " + result);

                } else if (low.equals("bulkhead")) {
                    System.out.println(chatBulkhead   .getStats());
                    System.out.println(kvBulkhead     .getStats());
                    System.out.println(metricsBulkhead.getStats());

                } else if (low.equals("gossip")) {
                    System.out.println(gossip.summary());
                    gossip.getRecentLogs(5).forEach(System.out::println);

                } else if (low.equals("vc") || low.equals("vectorclock")) {
                    for (int i = 0; i < 3; i++)
                        System.out.println("  Node" + (i+1) + " VC: " + vcs[i]);
                    System.out.println("  N1 <-> N2: " + VectorClock.relation(vcs[0], vcs[1]));
                    System.out.println("  N1 <-> N3: " + VectorClock.relation(vcs[0], vcs[2]));

                } else if (low.startsWith("dup ")) {
                    RaftNodeImpl leader = findLeader(nodes);
                    if (leader == null) { System.out.println("لا يوجد قائد"); }
                    else {
                        String reqId = newId();
                        String cmd = "put " + line.substring(4);
                        System.out.println(">>> اول ارسال:  " + leader.submit(reqId, cmd));
                        System.out.println(">>> اعادة ارسال: " + leader.submit(reqId, cmd));
                    }

                } else if (low.startsWith("fail ")) {
                    int id = Integer.parseInt(line.substring(5).trim());
                    nodes.get(id).setFailing(true);
                    System.out.println(">>> Node " + id + " يفشل — اختبار Circuit Breaker");

                } else if (low.startsWith("recover ")) {
                    int id = Integer.parseInt(line.substring(8).trim());
                    nodes.get(id).setFailing(false);
                    System.out.println(">>> Node " + id + " تعافى");

                } else if (low.startsWith("r ")) {
                    int id = Integer.parseInt(line.substring(2).trim());
                    nodes.get(id).revive();
                    vcs[id - 1].receive(vcs[0].snapshot());
                    System.out.println(">>> احييت Node " + id + " | VC: " + vcs[id-1]);

                } else {
                    int id = Integer.parseInt(line);
                    nodes.get(id).crash();
                    System.out.println(">>> قتلت Node " + id);
                }
            } catch (Exception e) {
                System.out.println("امر غير صالح: " + e.getMessage());
            }
        }
    }

    /** بورت الويب: من متغير البيئة PORT (للنشر) وإلا 8080 محلياً */
    static int resolvePort() {
        String env = System.getenv("PORT");
        if (env != null && env.matches("\\d+")) return Integer.parseInt(env.trim());
        return 8080;
    }

    static String newId() { return UUID.randomUUID().toString().substring(0, 8); }

    static RaftNodeImpl findLeader(Map<Integer, RaftNodeImpl> nodes) {
        for (RaftNodeImpl n : nodes.values()) if (n.isLeader()) return n;
        return null;
    }

    static RaftNodeImpl startNode(int id, int port, List<Integer> peers) throws Exception {
        Registry registry = LocateRegistry.createRegistry(port);
        RaftNodeImpl node = new RaftNodeImpl(id, peers);
        registry.rebind("RaftNode", node);
        return node;
    }
}
