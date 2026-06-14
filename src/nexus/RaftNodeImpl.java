package nexus;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RaftNodeImpl extends UnicastRemoteObject implements RaftNode {

    enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final int id;
    private final List<Integer> peerPorts;
    private final Random random = new Random();
    private final Object lock = new Object();

    private Role role = Role.FOLLOWER;
    private long currentTerm = 0;
    private Integer votedFor = null;
    private long lastHeartbeat = System.currentTimeMillis();
    private long electionTimeout;
    private long lastHeartbeatSent = 0;
    private volatile boolean crashed = false;

    private List<LogEntry> log = new ArrayList<>();
    private int commitIndex = -1;
    private int lastApplied = -1;

    private final Map<String, String> kv = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> matchIndex = new HashMap<>();
    private final Set<String> applied = new HashSet<>();// مفاتيح الطلبات المطبَّقة (Idempotency)
    private final Set<Integer> blockedPorts = ConcurrentHashMap.newKeySet();   // محاكاة انقسام الشبكة
    private volatile boolean failing = false;
    protected RaftNodeImpl(int id, List<Integer> peerPorts) throws RemoteException {
        super();
        this.id = id;
        this.peerPorts = peerPorts;
        this.electionTimeout = randomTimeout();
    }

    private long randomTimeout() { return 1500 + random.nextInt(1500); }

    public void crash() { crashed = true; }
    public void revive() { crashed = false; synchronized (lock) { lastHeartbeat = System.currentTimeMillis(); } }
    public boolean isLeader() { synchronized (lock) { return role == Role.LEADER && !crashed; } }
    public boolean isAlive() { return !crashed; }
    public String get(String key) {
        if (failing) throw new RuntimeException("Node " + id + " فشل القراءة");
        return kv.get(key);
    }
    public void blockPort(int port) { blockedPorts.add(port); }
    public void unblockAll() { blockedPorts.clear(); }
    public void setFailing(boolean f) { failing = f; }
    public boolean isIsolated() { return !blockedPorts.isEmpty(); }
    public String getRoleLabel() { synchronized (lock) { return crashed ? "DOWN" : role.name(); } }
    public long getTerm() { synchronized (lock) { return currentTerm; } }
    public int getLogSize() { synchronized (lock) { return log.size(); } }
    public int getCommitted() { synchronized (lock) { return commitIndex + 1; } }
    public Map<String, String> snapshotKv() { return new LinkedHashMap<>(kv); }



    public String submit(String requestId, String command) {
        synchronized (lock) {
            if (role != Role.LEADER || crashed) return "هذه العقدة ليست القائد (" + role + ")";
            log.add(new LogEntry(currentTerm, requestId, command));
            return "أُضيف للسجل عند index " + (log.size() - 1) + " (reqId=" + requestId + ")";
        }
    }

    public String dumpState() {
        synchronized (lock) {
            return "Node " + id + " | " + (crashed ? "DOWN" : role)
                    + " | term " + currentTerm
                    + " | log=" + log.size() + " مُلتزَم=" + (commitIndex + 1)
                    + " | kv=" + kv;
        }
    }

    private int lastLogIndex() { return log.size() - 1; }
    private long lastLogTerm() { return log.isEmpty() ? 0 : log.get(log.size() - 1).term; }

    @Override
    public VoteResponse requestVote(long term, int candidateId, int candLastIdx, long candLastTerm) throws RemoteException {
        if (crashed) throw new RemoteException("Node " + id + " is down");
        synchronized (lock) {
            if (term > currentTerm) stepDown(term);
            boolean upToDate = (candLastTerm > lastLogTerm())
                    || (candLastTerm == lastLogTerm() && candLastIdx >= lastLogIndex());
            boolean granted = false;
            if (term == currentTerm && (votedFor == null || votedFor == candidateId) && upToDate) {
                votedFor = candidateId;
                lastHeartbeat = System.currentTimeMillis();
                granted = true;
                System.out.println("[Node " + id + "] منحت صوتي للعقدة " + candidateId + " (term " + term + ")");
            }
            return new VoteResponse(currentTerm, granted);
        }
    }

    @Override
    public AppendResponse appendEntries(long term, int leaderId, List<LogEntry> entries, int leaderCommit) throws RemoteException {
        if (crashed) throw new RemoteException("Node " + id + " is down");
        synchronized (lock) {
            if (term > currentTerm) stepDown(term);
            if (term < currentTerm) return new AppendResponse(currentTerm, false);

            role = Role.FOLLOWER;
            lastHeartbeat = System.currentTimeMillis();
            this.log = new ArrayList<>(entries);
            if (leaderCommit > commitIndex) {
                commitIndex = Math.min(leaderCommit, log.size() - 1);
                applyCommitted();
            }
            return new AppendResponse(currentTerm, true);
        }
    }

    private void stepDown(long term) {
        currentTerm = term;
        role = Role.FOLLOWER;
        votedFor = null;
    }

    public void run() {
        while (true) {
            try { Thread.sleep(100); } catch (InterruptedException e) { return; }
            if (crashed) continue;

            Role r; long since;
            synchronized (lock) { r = role; since = System.currentTimeMillis() - lastHeartbeat; }

            if (r == Role.LEADER) {
                if (System.currentTimeMillis() - lastHeartbeatSent >= 400) {
                    sendHeartbeats();
                    lastHeartbeatSent = System.currentTimeMillis();
                }
            } else if (since >= electionTimeout) {
                startElection();
            }
        }
    }

    private void startElection() {
        long term; int lastIdx; long lastTerm; int totalNodes = peerPorts.size() + 1;
        synchronized (lock) {
            currentTerm++;
            role = Role.CANDIDATE;
            votedFor = id;
            lastHeartbeat = System.currentTimeMillis();
            electionTimeout = randomTimeout();
            term = currentTerm;
            lastIdx = lastLogIndex();
            lastTerm = lastLogTerm();
        }
        System.out.println("[Node " + id + "] ⏱ انتهت المهلة — بدأت انتخاباً (term " + term + ")");

        int votes = 1;
        for (int port : peerPorts) {
            try {
                VoteResponse resp = lookupPeer(port).requestVote(term, id, lastIdx, lastTerm);
                synchronized (lock) {
                    if (resp.term > currentTerm) { stepDown(resp.term); return; }
                    if (role != Role.CANDIDATE || currentTerm != term) return;
                }
                if (resp.voteGranted) votes++;
            } catch (Exception ignored) { }
        }

        synchronized (lock) {
            if (role == Role.CANDIDATE && currentTerm == term && votes > totalNodes / 2) {
                role = Role.LEADER;
                matchIndex.clear();
                lastHeartbeatSent = 0;
                System.out.println("\n★★★ [Node " + id + "] فزت بالانتخاب! أنا القائد (term " + currentTerm + ") — أصوات " + votes + "/" + totalNodes + " ★★★\n");
            }
        }
    }

    private void sendHeartbeats() {
        long term; List<LogEntry> snapshot; int leaderCommit;
        synchronized (lock) {
            if (role != Role.LEADER) return;
            term = currentTerm;
            snapshot = new ArrayList<>(log);
            leaderCommit = commitIndex;
        }
        int sentLastIndex = snapshot.size() - 1;

        for (int port : peerPorts) {
            try {
                AppendResponse resp = lookupPeer(port).appendEntries(term, id, snapshot, leaderCommit);
                synchronized (lock) {
                    if (resp.term > currentTerm) {
                        System.out.println("[Node " + id + "] اكتشفت term أعلى — تنازلت عن القيادة");
                        stepDown(resp.term);
                        return;
                    }
                    if (resp.success) matchIndex.put(port, sentLastIndex);
                }
            } catch (Exception ignored) { }
        }

        synchronized (lock) { if (role == Role.LEADER) advanceCommit(); }
    }

    private void advanceCommit() {
        int n = lastLogIndex();
        if (n < 0 || log.get(n).term != currentTerm) return;
        int count = 1;
        for (int port : peerPorts) {
            Integer m = matchIndex.get(port);
            if (m != null && m >= n) count++;
        }
        if (count > (peerPorts.size() + 1) / 2 && n > commitIndex) {
            commitIndex = n;
            applyCommitted();
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry e = log.get(lastApplied);
            if (applied.contains(e.requestId)) {
                System.out.println("[Node " + id + "] ⊘ تجاهلت طلباً مكرّراً (index " + lastApplied + "): " + e.command);
                continue;
            }
            applied.add(e.requestId);

            String[] p = e.command.split("\\s+", 3);
            if (p.length >= 3 && p[0].equalsIgnoreCase("put")) kv.put(p[1], p[2]);
            else if (p.length >= 2 && p[0].equalsIgnoreCase("del")) kv.remove(p[1]);
            System.out.println("[Node " + id + "] ✓ التزمت وطبّقت (index " + lastApplied + "): " + e.command);
        }



    }

    private RaftNode lookupPeer(int port) throws Exception {
        if (blockedPorts.contains(port)) throw new Exception("partitioned from " + port);
        Registry reg = LocateRegistry.getRegistry("localhost", port);
        return (RaftNode) reg.lookup("RaftNode");
    }}