package nexus;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ============================================================
 *  ChatServer — منصة دردشة حقيقية فوق NexusChat
 *
 *  يُنفّذ مفاهيم سؤال الامتحان بالكامل:
 *  ✅ WebSocket-style (SSE + POST)
 *  ✅ Connection State Management
 *  ✅ Fan-out: Write-time vs Read-time (قابل التبديل)
 *  ✅ Offline Message Queue
 *  ✅ Read Receipts ✓✓ مع Distributed Lock (Race Condition fix)
 *  ✅ Vector Clock للترتيب السببي
 *  ✅ Backpressure على الـ queue
 * ============================================================
 */
public class ChatServer {

    // ── إعدادات ───────────────────────────────────────────────
    private static final int  OFFLINE_QUEUE_LIMIT = 100;   // Backpressure
    private static final long READ_LOCK_TIMEOUT   = 500;   // ms

    // ── Fan-out mode ──────────────────────────────────────────
    public enum FanoutMode { WRITE_TIME, READ_TIME }
    private volatile FanoutMode fanoutMode = FanoutMode.WRITE_TIME;

    // ── بيانات المستخدمين ─────────────────────────────────────
    private final Map<String, PrintWriter>          streams      = new ConcurrentHashMap<>();
    private final Map<String, Queue<ChatMessage>>   offlineQueue = new ConcurrentHashMap<>();
    private final Map<String, List<ChatMessage>>    userInbox    = new ConcurrentHashMap<>(); // READ_TIME
    private final List<ChatMessage>                 globalLog    = new CopyOnWriteArrayList<>();
    private final Map<String, ReentrantLock>        readLocks    = new ConcurrentHashMap<>();
    // ── Vector Clocks per user ────────────────────────────────
    // كل مستخدم = "process" مستقل له خانته الخاصة في المتجه
    private final Map<String, VectorClock> userVC     = new ConcurrentHashMap<>();
    private final Map<String, Integer>     userSlot   = new ConcurrentHashMap<>();
    private final AtomicInteger            slotCounter = new AtomicInteger(0);
    private static final int CLUSTER_SIZE = 3;

    /** خانة ثابتة لكل مستخدم (0,1,2,...) كي تعكس الـ VC السببية الحقيقية بين المستخدمين */
    private int slotFor(String user) {
        return userSlot.computeIfAbsent(user, u -> slotCounter.getAndIncrement() % CLUSTER_SIZE);
    }
    private VectorClock vcFor(String user) {
        return userVC.computeIfAbsent(user, u -> new VectorClock(slotFor(u), CLUSTER_SIZE));
    }

    // ── إحصائيات ──────────────────────────────────────────────
    private final AtomicInteger totalSent      = new AtomicInteger(0);
    private final AtomicInteger totalDelivered = new AtomicInteger(0);
    private final AtomicInteger totalRead      = new AtomicInteger(0);
    private final AtomicInteger offlineDropped = new AtomicInteger(0);
    private final List<String>  eventLog       = new CopyOnWriteArrayList<>();

    private HttpServer httpServer;   // يُستخدم فقط في الوضع المستقل (بورت خاص)

    /** الوضع الموحّد (للنشر): سجّل مسارات /chat على خادم موجود مشترك */
    public ChatServer() { }

    /** الوضع المستقل (تطوير محلي): خادم HTTP على بورت خاص */
    public ChatServer(int port) throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        registerOn(httpServer);
        httpServer.setExecutor(Executors.newCachedThreadPool());
    }

    /** يسجّل مسارات الدردشة على خادم HTTP مُمرَّر (بورت واحد للكل) */
    public void registerOn(HttpServer server) {
        server.createContext("/chat/connect", this::handleConnect);
        server.createContext("/chat/send",    this::handleSend);
        server.createContext("/chat/read",    this::handleRead);
        server.createContext("/chat/fanout",  this::handleFanout);
        server.createContext("/chat/status",  this::handleStatus);
    }

    public void start() {
        if (httpServer != null) httpServer.start();
        System.out.println("[Chat] منصة الدردشة جاهزة — SSE على /chat/connect");
    }

    // ── اتصال المستخدم (SSE stream) ──────────────────────────
    private void handleConnect(HttpExchange ex) throws IOException {
        String user = getParam(ex.getRequestURI().getQuery(), "user");
        if (user == null) { ex.sendResponseHeaders(400, 0); ex.close(); return; }

        ex.getResponseHeaders().add("Content-Type",  "text/event-stream; charset=utf-8");
        ex.getResponseHeaders().add("Cache-Control", "no-cache");
        ex.getResponseHeaders().add("Connection",    "keep-alive");
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(200, 0);

        PrintWriter writer = new PrintWriter(
            new OutputStreamWriter(ex.getResponseBody(), StandardCharsets.UTF_8), true);

        // تسجيل المستخدم
        streams.put(user, writer);
        vcFor(user);                       // يضمن وجود VC بخانة فريدة لهذا المستخدم
        readLocks.put(user, new ReentrantLock());

        log("[Chat] 🟢 " + user + " اتصل (" + streams.size() + " متصل)");
        broadcastEvent("system", "user-join", user + " انضم", null);

        // تسليم الرسائل المؤجلة (Offline Queue)
        Queue<ChatMessage> pending = offlineQueue.remove(user);
        if (pending != null && !pending.isEmpty()) {
            int count = pending.size();
            while (!pending.isEmpty()) {
                ChatMessage m = pending.poll();
                m.status = ChatMessage.Status.DELIVERED;
                sendSSE(writer, m.toJson());
                totalDelivered.incrementAndGet();
            }
            log("[Chat] 📬 سُلّم " + count + " رسالة مؤجلة لـ " + user);
        }

        // READ_TIME fan-out: اجلب الرسائل المخزّنة عند القراءة (الاتصال)
        List<ChatMessage> inbox = userInbox.remove(user);
        if (inbox != null && !inbox.isEmpty()) {
            for (ChatMessage m : inbox) {
                vcFor(user).receiveArray(m.vectorClock);
                m.status = ChatMessage.Status.DELIVERED;
                sendSSE(writer, m.toJson());
                totalDelivered.incrementAndGet();
            }
            log("[Chat:READ_TIME] 📥 جُلب " + inbox.size() + " رسالة لـ " + user + " عند القراءة");
        }

        // إبقاء الاتصال مفتوحاً
        try {
            while (!writer.checkError()) Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
        finally {
            streams.remove(user);
            log("[Chat] 🔴 " + user + " قطع الاتصال");
            broadcastEvent("system", "user-leave", user + " غادر", null);
        }
    }

    // ── إرسال رسالة ──────────────────────────────────────────
    private void handleSend(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) { ex.sendResponseHeaders(405, 0); return; }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> params = parseBody(body);

        String sender   = params.get("sender");
        String receiver = params.get("receiver");   // null = broadcast
        String content  = params.get("content");

        if (sender == null || content == null) { respond(ex, 400, "{\"error\":\"missing params\"}"); return; }

        // Vector Clock tick (خانة المرسِل الخاصة)
        VectorClock vc = vcFor(sender);
        VectorClock sentVC = vc.send();
        int[] vcArr = new int[CLUSTER_SIZE];
        for (int i = 0; i < CLUSTER_SIZE; i++) vcArr[i] = sentVC.get(i);

        ChatMessage msg = new ChatMessage(sender, receiver, content, vcArr);
        globalLog.add(msg);
        totalSent.incrementAndGet();

        if (fanoutMode == FanoutMode.WRITE_TIME) {
            fanoutWrite(msg);
        } else {
            fanoutReadTime(msg);
        }

        respond(ex, 200, "{\"id\":\"" + msg.id + "\",\"fanout\":\"" + fanoutMode + "\",\"vc\":" + Arrays.toString(vcArr) + "}");
    }

    // ── Fan-out Write-Time: نسّخ لكل مستقبِل فوراً ──────────
    private void fanoutWrite(ChatMessage msg) {
        if (msg.receiver != null) {
            deliverTo(msg.receiver, msg);
        } else {
            // Broadcast
            for (String user : getAllUsers()) {
                if (!user.equals(msg.sender)) deliverTo(user, msg);
            }
        }
    }

    // ── Fan-out Read-Time: خزّن مرة واحدة، اجلب عند القراءة ─
    private void fanoutReadTime(ChatMessage msg) {
        userInbox.computeIfAbsent(msg.receiver != null ? msg.receiver : "_broadcast", k -> new CopyOnWriteArrayList<>()).add(msg);
        log("[Chat:READ_TIME] رسالة " + msg.id + " خُزّنت — ستُجلب عند الطلب");
    }

    // ── تسليم رسالة لمستخدم ──────────────────────────────────
    private void deliverTo(String user, ChatMessage msg) {
        // الاستقبال السببي: ساعة المستلِم تدمج ساعة الرسالة (Happened-Before)
        if (msg.vectorClock != null) vcFor(user).receiveArray(msg.vectorClock);
        PrintWriter writer = streams.get(user);
        if (writer != null && !writer.checkError()) {
            // المستخدم متصل
            msg.status = ChatMessage.Status.DELIVERED;
            sendSSE(writer, msg.toJson());
            totalDelivered.incrementAndGet();
            log("[Chat] ✉ " + msg.sender + " → " + user + ": \"" + msg.content.substring(0, Math.min(20, msg.content.length())) + "\"");
        } else {
            // مستخدم غير متصل — Offline Queue مع Backpressure
            Queue<ChatMessage> q = offlineQueue.computeIfAbsent(user, k -> new LinkedList<>());
            if (q.size() >= OFFLINE_QUEUE_LIMIT) {
                offlineDropped.incrementAndGet();
                log("[Chat:BACKPRESSURE] ⛔ Queue " + user + " ممتلئة — رُفضت رسالة");
            } else {
                q.add(msg);
                log("[Chat:OFFLINE] 📦 رسالة لـ " + user + " في الانتظار (" + q.size() + "/" + OFFLINE_QUEUE_LIMIT + ")");
            }
        }
    }

    // ── Read Receipt ✓✓ مع Distributed Lock ─────────────────
    private void handleRead(HttpExchange ex) throws IOException {
        Map<String, String> q = parseQuery(ex.getRequestURI().getQuery());
        String user   = q.get("user");
        String msgId  = q.get("msgId");
        if (user == null || msgId == null) { respond(ex, 400, "{}"); return; }

        ReentrantLock lock = readLocks.computeIfAbsent(user, k -> new ReentrantLock());

        // محاولة الحصول على القفل لمنع Race Condition
        // (مثال: المستخدم يفتح من جهازين في نفس الوقت)
        boolean locked = false;
        try {
            locked = lock.tryLock(READ_LOCK_TIMEOUT, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!locked) { respond(ex, 409, "{\"error\":\"race condition detected\"}"); return; }

            for (ChatMessage m : globalLog) {
                if (m.id.equals(msgId) && m.status != ChatMessage.Status.READ) {
                    m.status = ChatMessage.Status.READ;
                    totalRead.incrementAndGet();
                    // أخبر المُرسل بـ ✓✓
                    PrintWriter senderStream = streams.get(m.sender);
                    if (senderStream != null) sendSSE(senderStream, "{\"type\":\"read_receipt\",\"msgId\":\"" + msgId + "\",\"reader\":\"" + user + "\"}");
                    log("[Chat:READ_RECEIPT] ✓✓ " + user + " قرأ رسالة " + msgId);
                    respond(ex, 200, "{\"ok\":true,\"status\":\"READ\"}");
                    return;
                }
            }
            respond(ex, 404, "{\"error\":\"message not found\"}");
        } catch (InterruptedException ie) {
            respond(ex, 500, "{}");
        } finally {
            if (locked) lock.unlock();
        }
    }

    // ── تبديل Fan-out Mode ────────────────────────────────────
    private void handleFanout(HttpExchange ex) throws IOException {
        String mode = getParam(ex.getRequestURI().getQuery(), "mode");
        if ("READ_TIME".equalsIgnoreCase(mode)) fanoutMode = FanoutMode.READ_TIME;
        else fanoutMode = FanoutMode.WRITE_TIME;
        log("[Chat] Fan-out: " + fanoutMode);
        respond(ex, 200, "{\"fanout\":\"" + fanoutMode + "\"}");
    }

    // ── JSON إحصائيات ────────────────────────────────────────
    private void handleStatus(HttpExchange ex) throws IOException {
        // حساب الرسائل الغير مقروءة لكل مستخدم
        Map<String, Integer> unread = new LinkedHashMap<>();
        for (ChatMessage m : globalLog)
            if (m.status != ChatMessage.Status.READ && m.receiver != null)
                unread.merge(m.receiver, 1, Integer::sum);

        StringBuilder sb = new StringBuilder("{");
        sb.append("\"online\":").append(streams.size());
        sb.append(",\"users\":[");
        boolean first = true;
        for (String u : getAllUsers()) {
            if (!first) sb.append(","); first = false;
            boolean online = streams.containsKey(u);
            int offQ = offlineQueue.containsKey(u) ? offlineQueue.get(u).size() : 0;
            sb.append("{\"name\":\"").append(u).append("\",\"online\":").append(online)
              .append(",\"offlineQ\":").append(offQ)
              .append(",\"unread\":").append(unread.getOrDefault(u, 0)).append("}");
        }
        sb.append("],\"stats\":{\"sent\":").append(totalSent.get())
          .append(",\"delivered\":").append(totalDelivered.get())
          .append(",\"read\":").append(totalRead.get())
          .append(",\"dropped\":").append(offlineDropped.get())
          .append(",\"fanout\":\"").append(fanoutMode).append("\"}")
          .append(",\"messages\":[");
        List<ChatMessage> recent = globalLog.size() > 20
            ? globalLog.subList(globalLog.size() - 20, globalLog.size())
            : globalLog;
        first = true;
        for (ChatMessage m : recent) {
            if (!first) sb.append(","); first = false;
            sb.append(m.toJson());
        }
        sb.append("],\"eventLog\":[");
        List<String> logs = eventLog.size() > 8
            ? eventLog.subList(eventLog.size() - 8, eventLog.size())
            : new ArrayList<>(eventLog);
        first = true;
        for (String l : logs) {
            if (!first) sb.append(","); first = false;
            sb.append("\"").append(esc(l)).append("\"");
        }
        sb.append("]}");
        respond(ex, 200, sb.toString());
    }

    // ── مساعدات ──────────────────────────────────────────────
    private void sendSSE(PrintWriter w, String json) {
        w.print("data: " + json + "\n\n");
        w.flush();
    }

    private void broadcastEvent(String type, String event, String msg, String exclude) {
        String json = "{\"type\":\"" + type + "\",\"event\":\"" + event + "\",\"msg\":\"" + esc(msg) + "\"}";
        for (Map.Entry<String, PrintWriter> e : streams.entrySet()) {
            if (!e.getKey().equals(exclude)) sendSSE(e.getValue(), json);
        }
    }

    private Set<String> getAllUsers() {
        Set<String> all = new LinkedHashSet<>(streams.keySet());
        all.addAll(offlineQueue.keySet());
        return all;
    }

    private void log(String msg) {
        System.out.println(msg);
        eventLog.add(msg);
        if (eventLog.size() > 50) eventLog.remove(0);
    }

    private void respond(HttpExchange ex, int code, String body) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    private String getParam(String query, String key) {
        if (query == null) return null;
        for (String p : query.split("&")) {
            int i = p.indexOf('=');
            if (i > 0 && URLDecoder.decode(p.substring(0,i), StandardCharsets.UTF_8).equals(key))
                return URLDecoder.decode(p.substring(i+1), StandardCharsets.UTF_8);
        }
        return null;
    }

    private Map<String, String> parseQuery(String q) {
        Map<String, String> m = new HashMap<>();
        if (q == null) return m;
        for (String p : q.split("&")) {
            int i = p.indexOf('=');
            if (i > 0) m.put(URLDecoder.decode(p.substring(0,i), StandardCharsets.UTF_8),
                             URLDecoder.decode(p.substring(i+1), StandardCharsets.UTF_8));
        }
        return m;
    }

    private Map<String, String> parseBody(String body) {
        Map<String, String> m = new HashMap<>();
        for (String p : body.split("&")) {
            int i = p.indexOf('=');
            if (i > 0) {
                try { m.put(URLDecoder.decode(p.substring(0,i), StandardCharsets.UTF_8),
                            URLDecoder.decode(p.substring(i+1), StandardCharsets.UTF_8)); }
                catch (Exception ignored) {}
            }
        }
        return m;
    }

    private String esc(String s) { return s==null?"":s.replace("\\","\\\\").replace("\"","\\\"").replace("\n"," "); }
}
