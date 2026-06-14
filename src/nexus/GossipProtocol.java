package nexus;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ============================================================
 *  Gossip Protocol — بروتوكول النميمة
 *  موزعة-2: Membership Management + Failure Detection
 *
 *  المفهوم:
 *   كل عقدة كل فترة تختار عشوائياً عقدة أخرى وتُشارك معها:
 *     - قائمة العقد الحيّة/الميتة (Membership)
 *     - الـ heartbeat counter الخاص بها
 *   إذا لم يتحدث counter عقدة لفترة → يُعتبر مشتبهاً (SUSPECT)
 *   إذا مضى وقت أطول → يُعتبر ميتاً (DEAD)
 *
 *  الفرق عن Raft heartbeat:
 *   - Gossip: لامركزي، كل العقد تتحدث لكل العقد
 *   - Raft:   مركزي، القائد فقط يُرسل heartbeat
 * ============================================================
 */
public class GossipProtocol {

    public enum NodeStatus { ALIVE, SUSPECT, DEAD }

    public static class NodeInfo {
        public final int     nodeId;
        public volatile long heartbeatCounter;
        public volatile long lastUpdated;
        public volatile NodeStatus status;

        NodeInfo(int nodeId) {
            this.nodeId           = nodeId;
            this.heartbeatCounter = 0;
            this.lastUpdated      = System.currentTimeMillis();
            this.status           = NodeStatus.ALIVE;
        }

        @Override public String toString() {
            return "Node" + nodeId + "[" + status + " hb=" + heartbeatCounter + "]";
        }
    }

    // ── إعدادات ───────────────────────────────────────────────
    private static final long GOSSIP_INTERVAL_MS  = 500;
    private static final long SUSPECT_TIMEOUT_MS  = 2000;
    private static final long DEAD_TIMEOUT_MS     = 6000;
    private static final int  FANOUT              = 2;   // كم عقدة نُخبر في كل دورة

    private final int                       localId;
    private final Map<Integer, RaftNodeImpl> raftNodes;
    private final Map<Integer, NodeInfo>     memberList = new ConcurrentHashMap<>();
    private final Random                     rng        = new Random();
    private final List<String>               gossipLog  = new CopyOnWriteArrayList<>();
    private volatile boolean                 running    = false;

    public GossipProtocol(int localId, Map<Integer, RaftNodeImpl> raftNodes) {
        this.localId    = localId;
        this.raftNodes  = raftNodes;
        for (Integer id : raftNodes.keySet())
            memberList.put(id, new NodeInfo(id));
    }

    // ── تشغيل ─────────────────────────────────────────────────
    public void start() {
        running = true;
        Thread t = new Thread(this::gossipLoop, "gossip-node-" + localId);
        t.setDaemon(true);
        t.start();
        System.out.println("[Gossip] 👂 بروتوكول النميمة بدأ (localId=" + localId + ")");
    }

    public void stop() { running = false; }

    private void gossipLoop() {
        while (running) {
            try {
                Thread.sleep(GOSSIP_INTERVAL_MS);
                incrementHeartbeat();
                detectFailures();
                spreadGossip();
            } catch (InterruptedException e) { break; }
        }
    }

    // ── زيادة الـ heartbeat المحلي ────────────────────────────
    private void incrementHeartbeat() {
        NodeInfo me = memberList.get(localId);
        if (me != null) {
            me.heartbeatCounter++;
            me.lastUpdated = System.currentTimeMillis();
            me.status = NodeStatus.ALIVE;
        }
    }

    // ── كشف الإخفاقات ─────────────────────────────────────────
    private void detectFailures() {
        long now = System.currentTimeMillis();
        for (NodeInfo info : memberList.values()) {
            if (info.nodeId == localId) continue;
            long silence = now - info.lastUpdated;
            NodeStatus prev = info.status;

            // تحقق من حالة Raft الفعلية إذا أمكن
            RaftNodeImpl raftNode = raftNodes.get(info.nodeId);
            if (raftNode != null && !raftNode.isAlive()) {
                info.status = NodeStatus.DEAD;
            } else if (silence > DEAD_TIMEOUT_MS) {
                info.status = NodeStatus.DEAD;
            } else if (silence > SUSPECT_TIMEOUT_MS) {
                info.status = NodeStatus.SUSPECT;
            } else {
                info.status = NodeStatus.ALIVE;
            }

            if (info.status != prev) {
                String msg = String.format("[Gossip] 🔔 Node%d: %s → %s (صمت %dms)",
                        info.nodeId, prev, info.status, silence);
                System.out.println(msg);
                addLog(msg);
            }
        }
    }

    // ── نشر المعلومات ─────────────────────────────────────────
    private void spreadGossip() {
        List<Integer> peers = new ArrayList<>(memberList.keySet());
        peers.remove((Integer) localId);
        Collections.shuffle(peers, rng);

        int count = 0;
        for (Integer peerId : peers) {
            if (count >= FANOUT) break;
            mergeFrom(peerId);
            count++;
        }
    }

    /** دمج المعلومات القادمة من عقدة مجاورة */
    private void mergeFrom(int peerId) {
        NodeInfo peer = memberList.get(peerId);
        if (peer == null) return;
        // محاكاة: نحدّث وقت آخر تواصل إذا العقدة حيّة في Raft
        RaftNodeImpl raftNode = raftNodes.get(peerId);
        if (raftNode != null && raftNode.isAlive()) {
            peer.heartbeatCounter++;
            peer.lastUpdated = System.currentTimeMillis();
        }
    }

    // ── قراءة الحالة ─────────────────────────────────────────
    public Map<Integer, NodeInfo> getMemberList() {
        return Collections.unmodifiableMap(memberList);
    }

    public NodeStatus getStatus(int nodeId) {
        NodeInfo info = memberList.get(nodeId);
        return info == null ? NodeStatus.DEAD : info.status;
    }

    public List<String> getRecentLogs(int n) {
        List<String> all = new ArrayList<>(gossipLog);
        return all.subList(Math.max(0, all.size() - n), all.size());
    }

    private void addLog(String msg) {
        gossipLog.add(msg);
        if (gossipLog.size() > 200) gossipLog.remove(0);
    }

    public String summary() {
        StringBuilder sb = new StringBuilder("[Gossip] عضوية الكلستر:\n");
        for (NodeInfo info : memberList.values())
            sb.append("  ").append(info).append("\n");
        return sb.toString();
    }
}
