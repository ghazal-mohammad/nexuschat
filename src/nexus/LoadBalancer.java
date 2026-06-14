package nexus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LoadBalancer {

    public enum Algorithm {
        ROUND_ROBIN, WEIGHTED_ROUND_ROBIN, LEAST_CONNECTIONS,
        LEAST_RESPONSE_TIME, STICKY_SESSION, CONSISTENT_HASH,
        POWER_OF_TWO, JOIN_IDLE_QUEUE
    }

    private volatile Algorithm algorithm = Algorithm.LEAST_CONNECTIONS;

    private final Map<Integer, RaftNodeImpl>   nodes;
    private final Map<Integer, AtomicInteger>  activeConnections = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger>  servedCount       = new ConcurrentHashMap<>();
    private final Map<Integer, CircuitBreaker> breakers          = new ConcurrentHashMap<>();
    private final Map<String,  String>         lastGood          = new ConcurrentHashMap<>();

    private final AtomicInteger rrIndex  = new AtomicInteger(0);
    private final AtomicInteger wrrIndex = new AtomicInteger(0);
    private final Map<Integer, Integer> weights      = new LinkedHashMap<>();
    private final List<Integer>         weightedList = new ArrayList<>();
    private final Map<Integer, AtomicLong> totalResponseMs = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> responseCount   = new ConcurrentHashMap<>();
    private final TreeMap<Integer, Integer> hashRing = new TreeMap<>();
    private static final int VIRTUAL_NODES = 150;
    private final Map<String, Integer> stickyMap = new ConcurrentHashMap<>();
    private final Queue<Integer> idleQueue   = new LinkedList<>();
    private final Set<Integer>   inIdleQueue = ConcurrentHashMap.newKeySet();
    private final Random rng = new Random();
    private MetricsCollector metrics = null;

    public LoadBalancer(Map<Integer, RaftNodeImpl> nodes) {
        this.nodes = nodes;
        for (Integer id : nodes.keySet()) {
            activeConnections.put(id, new AtomicInteger(0));
            servedCount      .put(id, new AtomicInteger(0));
            totalResponseMs  .put(id, new AtomicLong(0));
            responseCount    .put(id, new AtomicLong(0));
            breakers         .put(id, new CircuitBreaker(3, 5000));
            weights          .put(id, 1);
        }
        rebuildWeightedList();
        buildHashRing();
    }

    public void setMetrics(MetricsCollector mc) { this.metrics = mc; }

    public void setAlgorithm(Algorithm alg) {
        this.algorithm = alg;
        System.out.println("[LB] الخوارزمية: " + alg);
    }

    public Algorithm getAlgorithm() { return algorithm; }

    public void setWeight(int nodeId, int w) {
        weights.put(nodeId, Math.max(1, w));
        rebuildWeightedList();
        buildHashRing();
        System.out.println("[LB] وزن Node " + nodeId + " -> " + w);
    }

    public String read(String key) { return read(key, key); }

    public String read(String key, String clientId) {
        Integer target = pick(clientId);
        if (target == null) return "لا توجد عقدة متاحة";
        System.out.println("[LB:" + algorithm.name() + "] قراءة '" + key + "' -> Node " + target);
        String v = serveRead(target, key);
        return v != null ? v : "(Fallback)";
    }

    public String write(String reqId, String command) {
        for (RaftNodeImpl n : nodes.values())
            if (n.isLeader()) { System.out.println("[LB] كتابة -> القائد"); return n.submit(reqId, command); }
        return "لا يوجد قائد";
    }

    public void loadTest(int count, String key) throws InterruptedException {
        for (AtomicInteger a : servedCount.values()) a.set(0);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final String clientId = "client-" + (i % 5);
            Thread t = new Thread(() -> { Integer tgt = pick(clientId); if (tgt != null) serveRead(tgt, key); });
            threads.add(t); t.start();
        }
        for (Thread t : threads) t.join();
        System.out.println("[LB:" + algorithm + "] توزيع " + count + " طلب:");
        for (Integer id : nodes.keySet())
            System.out.printf("   Node %d %s | CB:%s -> %d req | avg:%.1fms%n",
                    id, nodes.get(id).isAlive() ? "" : "(DOWN)",
                    breakers.get(id).state(), servedCount.get(id).get(), avgResponse(id));
    }

    public Integer pick(String clientOrKey) {
        switch (algorithm) {
            case ROUND_ROBIN:          return pickRoundRobin();
            case WEIGHTED_ROUND_ROBIN: return pickWeightedRoundRobin();
            case LEAST_CONNECTIONS:    return pickLeastConnections();
            case LEAST_RESPONSE_TIME:  return pickLeastResponseTime();
            case STICKY_SESSION:       return pickSticky(clientOrKey);
            case CONSISTENT_HASH:      return pickConsistentHash(clientOrKey);
            case POWER_OF_TWO:         return pickPowerOfTwo();
            case JOIN_IDLE_QUEUE:      return pickJoinIdleQueue();
            default:                   return pickLeastConnections();
        }
    }

    // 1. Round-Robin
    private Integer pickRoundRobin() {
        List<Integer> alive = aliveAllowed();
        if (alive.isEmpty()) return null;
        return alive.get(Math.abs(rrIndex.getAndIncrement()) % alive.size());
    }

    // 2. Weighted Round-Robin
    private Integer pickWeightedRoundRobin() {
        List<Integer> alive = aliveAllowed();
        if (alive.isEmpty()) return null;
        List<Integer> filtered = new ArrayList<>();
        for (int id : weightedList) if (alive.contains(id)) filtered.add(id);
        if (filtered.isEmpty()) return pickRoundRobin();
        return filtered.get(Math.abs(wrrIndex.getAndIncrement()) % filtered.size());
    }

    // 3. Least Connections
    private Integer pickLeastConnections() {
        Integer best = null; int min = Integer.MAX_VALUE;
        for (Integer id : aliveAllowed()) {
            int c = activeConnections.get(id).get();
            if (c < min) { min = c; best = id; }
        }
        return best;
    }

    // 4. Least Response Time
    private Integer pickLeastResponseTime() {
        Integer best = null; double minAvg = Double.MAX_VALUE;
        for (Integer id : aliveAllowed()) {
            double avg = avgResponse(id);
            if (avg < minAvg) { minAvg = avg; best = id; }
        }
        return best;
    }

    // 5. Sticky Session
    private Integer pickSticky(String clientId) {
        if (clientId == null) return pickRoundRobin();
        List<Integer> alive = aliveAllowed();
        if (alive.isEmpty()) return null;
        Integer pinned = stickyMap.get(clientId);
        if (pinned != null && alive.contains(pinned)) return pinned;
        Integer chosen = alive.get(Math.abs(clientId.hashCode()) % alive.size());
        stickyMap.put(clientId, chosen);
        System.out.println("[LB:STICKY] '" + clientId + "' -> Node " + chosen);
        return chosen;
    }

    // 6. Consistent Hashing
    private void buildHashRing() {
        hashRing.clear();
        for (Map.Entry<Integer, Integer> e : weights.entrySet()) {
            int id = e.getKey(), vnodes = VIRTUAL_NODES * e.getValue();
            for (int v = 0; v < vnodes; v++) hashRing.put(fnv("node-" + id + "-vnode-" + v), id);
        }
    }

    private Integer pickConsistentHash(String key) {
        if (hashRing.isEmpty()) return null;
        List<Integer> alive = aliveAllowed();
        if (alive.isEmpty()) return null;
        int h = fnv(key != null ? key : "default");
        NavigableMap<Integer, Integer> tail = hashRing.tailMap(h, true);
        for (Map.Entry<Integer, Integer> e : (tail.isEmpty() ? hashRing : tail).entrySet())
            if (alive.contains(e.getValue())) return e.getValue();
        for (Map.Entry<Integer, Integer> e : hashRing.entrySet())
            if (alive.contains(e.getValue())) return e.getValue();
        return null;
    }

    private static int fnv(String s) {
        int h = 0x811c9dc5;
        for (char c : s.toCharArray()) { h ^= c; h *= 0x01000193; }
        return h;
    }

    // 7. Power of Two Choices
    private Integer pickPowerOfTwo() {
        List<Integer> alive = aliveAllowed();
        if (alive.isEmpty()) return null;
        if (alive.size() == 1) return alive.get(0);
        int i1 = rng.nextInt(alive.size()), i2;
        do { i2 = rng.nextInt(alive.size()); } while (i2 == i1);
        Integer a = alive.get(i1), b = alive.get(i2);
        return activeConnections.get(a).get() <= activeConnections.get(b).get() ? a : b;
    }

    // 8. Join-Idle-Queue
    private void notifyIdle(int nodeId) {
        if (!inIdleQueue.contains(nodeId) && nodes.get(nodeId).isAlive() && breakers.get(nodeId).allow()) {
            synchronized (idleQueue) { idleQueue.add(nodeId); inIdleQueue.add(nodeId); }
        }
    }

    private Integer pickJoinIdleQueue() {
        synchronized (idleQueue) {
            while (!idleQueue.isEmpty()) {
                Integer id = idleQueue.poll(); inIdleQueue.remove(id);
                if (nodes.get(id).isAlive() && breakers.get(id).allow()) return id;
            }
        }
        return pickLeastConnections();
    }

    private String serveRead(int target, String key) {
        CircuitBreaker cb = breakers.get(target);
        activeConnections.get(target).incrementAndGet();
        long start = System.currentTimeMillis();
        try {
            servedCount.get(target).incrementAndGet();
            Thread.sleep(3 + rng.nextInt(8));
            String v = nodes.get(target).get(key);
            cb.success();
            long ms = System.currentTimeMillis() - start;
            totalResponseMs.get(target).addAndGet(ms);
            responseCount  .get(target).incrementAndGet();
            if (metrics != null) metrics.record(target, ms);
            if (v != null) lastGood.put(key, v);
            return v;
        } catch (Exception e) {
            cb.failure();
            return lastGood.get(key);
        } finally {
            activeConnections.get(target).decrementAndGet();
            if (algorithm == Algorithm.JOIN_IDLE_QUEUE) notifyIdle(target);
        }
    }

    private List<Integer> aliveAllowed() {
        List<Integer> out = new ArrayList<>();
        for (Map.Entry<Integer, RaftNodeImpl> e : nodes.entrySet())
            if (e.getValue().isAlive() && breakers.get(e.getKey()).allow())
                out.add(e.getKey());
        return out;
    }

    private double avgResponse(int id) {
        long cnt = responseCount.get(id).get();
        return cnt == 0 ? 0.0 : (double) totalResponseMs.get(id).get() / cnt;
    }

    private void rebuildWeightedList() {
        weightedList.clear();
        for (Map.Entry<Integer, Integer> e : weights.entrySet())
            for (int i = 0; i < e.getValue(); i++) weightedList.add(e.getKey());
    }

    public Map<Integer, Integer> servedSnapshot() {
        Map<Integer, Integer> m = new LinkedHashMap<>();
        for (Map.Entry<Integer, AtomicInteger> e : servedCount.entrySet()) m.put(e.getKey(), e.getValue().get());
        return m;
    }

    public Map<Integer, Double> avgResponseSnapshot() {
        Map<Integer, Double> m = new LinkedHashMap<>();
        for (Integer id : nodes.keySet()) m.put(id, avgResponse(id));
        return m;
    }

    public Map<Integer, Integer> weightsSnapshot() { return new LinkedHashMap<>(weights); }

    public Map<Integer, String> circuitSnapshot() {
        Map<Integer, String> m = new LinkedHashMap<>();
        for (Map.Entry<Integer, CircuitBreaker> e : breakers.entrySet())
            m.put(e.getKey(), e.getValue().state().name());
        return m;
    }
}
