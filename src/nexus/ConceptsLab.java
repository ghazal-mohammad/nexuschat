package nexus;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * ============================================================
 *  ConceptsLab — مختبر مفاهيم الأنظمة الموزّعة (عروض حيّة شغّالة)
 *
 *  كل نقطة نهاية تُنفّذ محاكاة حقيقية وتُعيد سجلّاً خطوة بخطوة
 *  يُظهر الفرق بين النهجين بوضوح لا يقبل الشك:
 *
 *   /lab/ratelimit    — Token Bucket  vs  Sliding Window
 *   /lab/saga         — Orchestration vs  Choreography (Saga + Compensation)
 *   /lab/outbox       — Naive · Outbox · CDC (الكتابة المزدوجة)
 *   /lab/dlx          — Regular Queue vs  Dead-Letter Exchange
 *   /lab/comm         — Sync (REST)   vs  Async (Message Queue)
 *   /lab/consistency  — Strong (quorum) vs Eventual (replicas converge)
 * ============================================================
 */
public class ConceptsLab {

    public void registerOn(HttpServer server) {
        server.createContext("/lab/ratelimit",   this::handleRateLimit);
        server.createContext("/lab/saga",        this::handleSaga);
        server.createContext("/lab/outbox",      this::handleOutbox);
        server.createContext("/lab/dlx",         this::handleDlx);
        server.createContext("/lab/comm",        this::handleComm);
        server.createContext("/lab/consistency", this::handleConsistency);
        System.out.println("[Lab] مختبر المفاهيم جاهز — /lab/*");
    }

    // ═══════════════════════════════════════════════════════════════
    // 1) Rate Limiting — Token Bucket vs Sliding Window
    //    نُشغّل الخوارزميتين على *نفس* نمط الطلبات لإظهار الفرق:
    //    Token Bucket يسمح بدفعة أولية بحجم السعة ثم يُنظّم؛
    //    Sliding Window يفرض حدّاً صارماً لكل نافذة زمنية.
    // ═══════════════════════════════════════════════════════════════
    private void handleRateLimit(HttpExchange ex) throws IOException {
        Map<String,String> q = query(ex);
        int     n          = intParam(q, "n", 14);
        int     intervalMs = intParam(q, "intervalMs", 120);
        int     capacity   = intParam(q, "capacity", 5);     // Token Bucket
        double  refillRps  = intParam(q, "refillRps", 4);    // tokens/sec
        int     windowMs   = intParam(q, "windowMs", 1000);  // Sliding Window
        int     limit      = intParam(q, "limit", 5);

        StringBuilder tb = new StringBuilder("[");
        StringBuilder sw = new StringBuilder("[");
        int tbAllowed = 0, swAllowed = 0;

        // Token Bucket
        double tokens = capacity; long last = 0;
        for (int i = 0; i < n; i++) {
            long t = (long) i * intervalMs;
            tokens = Math.min(capacity, tokens + (t - last) / 1000.0 * refillRps);
            last = t;
            boolean allow = tokens >= 1.0;
            if (allow) { tokens -= 1.0; tbAllowed++; }
            if (i > 0) tb.append(",");
            tb.append("{\"t\":").append(t).append(",\"ok\":").append(allow).append("}");
        }
        tb.append("]");

        // Sliding Window (log)
        Deque<Long> hits = new ArrayDeque<>();
        for (int i = 0; i < n; i++) {
            long t = (long) i * intervalMs;
            while (!hits.isEmpty() && hits.peekFirst() <= t - windowMs) hits.pollFirst();
            boolean allow = hits.size() < limit;
            if (allow) { hits.addLast(t); swAllowed++; }
            if (i > 0) sw.append(",");
            sw.append("{\"t\":").append(t).append(",\"ok\":").append(allow).append("}");
        }
        sw.append("]");

        String json = "{"
            + "\"n\":" + n + ",\"intervalMs\":" + intervalMs
            + ",\"capacity\":" + capacity + ",\"refillRps\":" + (int) refillRps
            + ",\"windowMs\":" + windowMs + ",\"limit\":" + limit
            + ",\"tokenBucket\":{\"allowed\":" + tbAllowed + ",\"denied\":" + (n - tbAllowed) + ",\"reqs\":" + tb + "}"
            + ",\"slidingWindow\":{\"allowed\":" + swAllowed + ",\"denied\":" + (n - swAllowed) + ",\"reqs\":" + sw + "}"
            + ",\"note\":\"Token Bucket يسمح بدفعة بحجم السعة ثم يُنظّم بمعدّل التعبئة — جيّد للذرى القصيرة. Sliding Window يفرض حدّاً ثابتاً لكل نافذة — أعدل وأكثر صرامة.\""
            + "}";
        respond(ex, json);
    }

    // ═══════════════════════════════════════════════════════════════
    // 2) Saga — Orchestration vs Choreography (مع تعويض عند الفشل)
    //    رحلة: حجز طيران → دفع → حجز فندق. عند فشل خطوة تُنفَّذ
    //    التعويضات بترتيب عكسي (إلغاء ما تم).
    // ═══════════════════════════════════════════════════════════════
    private void handleSaga(HttpExchange ex) throws IOException {
        Map<String,String> q = query(ex);
        String mode = q.getOrDefault("mode", "orchestration");
        String fail = q.getOrDefault("fail", "none");        // none | flight | payment | hotel

        String[] steps = {"حجز الطيران", "خصم الدفع", "حجز الفندق"};
        String[] comps = {"إلغاء الطيران", "ردّ الدفع", "إلغاء الفندق"};
        String[] keys  = {"flight", "payment", "hotel"};
        boolean choreography = mode.equalsIgnoreCase("choreography");

        List<String> log = new ArrayList<>();
        int failedAt = -1;
        for (int i = 0; i < steps.length; i++) {
            boolean stepFails = keys[i].equalsIgnoreCase(fail);
            String actor = choreography
                ? "خدمة " + keys[i] + " (تتفاعل مع حدث)"
                : "المنسّق → خدمة " + keys[i];
            if (stepFails) {
                log.add(ev("action", steps[i], "FAILED", actor + " — فشلت العملية ✗"));
                failedAt = i; break;
            } else {
                log.add(ev("action", steps[i], "OK", actor + " — نجحت ✓"
                    + (choreography ? " وأطلقت حدثاً للتالية" : "")));
            }
        }
        boolean success = (failedAt == -1);
        if (!success) {
            log.add(ev("info", "بدء التعويض", "ROLLBACK",
                choreography ? "كل خدمة تتفاعل مع حدث الفشل وتُلغي عملها" : "المنسّق يشغّل التعويضات بترتيب عكسي"));
            for (int i = failedAt - 1; i >= 0; i--)
                log.add(ev("compensation", comps[i], "COMPENSATED", "أُلغيت العملية السابقة (Compensating Transaction)"));
        }

        String note = choreography
            ? "Choreography: لا منسّق مركزي — كل خدمة تنشر/تستهلك أحداثاً. اقتران أضعف وقابلية تطوّر أعلى، لكن تتبّع المسار أصعب."
            : "Orchestration: منسّق مركزي يقود الخطوات والتعويضات. تتبّع وتحكّم أوضح، لكنه نقطة تنسيق مركزية.";

        respond(ex, "{\"mode\":\"" + (choreography ? "choreography" : "orchestration")
            + "\",\"fail\":\"" + esc(fail) + "\",\"success\":" + success
            + ",\"log\":[" + String.join(",", log) + "],\"note\":\"" + esc(note) + "\"}");
    }

    // ═══════════════════════════════════════════════════════════════
    // 3) Outbox vs CDC — مشكلة الكتابة المزدوجة (Dual Write)
    //    الكتابة لقاعدة البيانات + نشر حدث. ماذا يحدث عند العطل بينهما؟
    // ═══════════════════════════════════════════════════════════════
    private void handleOutbox(HttpExchange ex) throws IOException {
        Map<String,String> q = query(ex);
        String mode  = q.getOrDefault("mode", "naive");      // naive | outbox | cdc
        boolean crash = Boolean.parseBoolean(q.getOrDefault("crash", "true"));

        List<String> log = new ArrayList<>();
        boolean consistent;
        switch (mode.toLowerCase()) {
            case "outbox" -> {
                log.add(ev("action", "معاملة واحدة", "OK", "كتابة الصفّ + صفّ Outbox ذرّياً (نفس الـ transaction)"));
                if (crash) log.add(ev("warn", "تعطّل التطبيق", "CRASH", "قبل النشر — لكن صفّ Outbox محفوظ في قاعدة البيانات"));
                log.add(ev("action", "Relay (poller)", "OK", "يقرأ Outbox بعد الإقلاع وينشر الحدث (تسليم مرّة واحدة على الأقل)"));
                log.add(ev("action", "وسيط الرسائل", "DELIVERED", "وصل الحدث — البيانات والحدث متطابقان"));
                consistent = true;
            }
            case "cdc" -> {
                log.add(ev("action", "كتابة قاعدة البيانات", "OK", "commit في سجلّ الـ WAL/binlog"));
                if (crash) log.add(ev("warn", "تعطّل التطبيق", "CRASH", "لا يهم — CDC يقرأ من سجلّ القاعدة لا من التطبيق"));
                log.add(ev("action", "CDC (Debezium نمطياً)", "OK", "يلتقط التغيير من سجلّ المعاملات وينشره"));
                log.add(ev("action", "وسيط الرسائل", "DELIVERED", "وصل الحدث تلقائياً — صفر كود نشر في التطبيق"));
                consistent = true;
            }
            default -> {   // naive dual-write
                log.add(ev("action", "كتابة قاعدة البيانات", "OK", "تمّ حفظ الصفّ"));
                if (crash) {
                    log.add(ev("warn", "تعطّل التطبيق", "CRASH", "بعد حفظ القاعدة وقبل نشر الحدث"));
                    log.add(ev("error", "نشر الحدث", "LOST", "لم يُنشر الحدث أبداً — قاعدة محدّثة وحدث مفقود ✗"));
                    consistent = false;
                } else {
                    log.add(ev("action", "نشر الحدث", "DELIVERED", "نجح هذه المرّة — لكنه غير ذرّي وعرضة للفقد"));
                    consistent = true;
                }
            }
        }
        String note = switch (mode.toLowerCase()) {
            case "outbox" -> "Outbox: تحويل الكتابة المزدوجة إلى كتابة واحدة ذرّية + Relay ينشر لاحقاً. موثوق بلا اعتماد على ميزات القاعدة.";
            case "cdc"    -> "CDC: لا كود نشر إطلاقاً — يلتقط التغييرات من سجلّ المعاملات. ألطف للتطبيق لكنه يحتاج بنية CDC.";
            default       -> "Naive: كتابتان منفصلتان (قاعدة ثم وسيط). أي عطل بينهما = عدم اتساق. هذه المشكلة التي يحلّها Outbox/CDC.";
        };
        respond(ex, "{\"mode\":\"" + esc(mode) + "\",\"crash\":" + crash
            + ",\"consistent\":" + consistent + ",\"log\":[" + String.join(",", log)
            + "],\"note\":\"" + esc(note) + "\"}");
    }

    // ═══════════════════════════════════════════════════════════════
    // 4) DLX vs Regular Queue — رسالة سامّة (Poison Message)
    // ═══════════════════════════════════════════════════════════════
    private void handleDlx(HttpExchange ex) throws IOException {
        Map<String,String> q = query(ex);
        boolean dlx = q.getOrDefault("mode", "dlx").equalsIgnoreCase("dlx");
        int maxRetries = intParam(q, "retries", 3);

        String[] msgs = {"msg#1", "msg#2 (سامّة)", "msg#3", "msg#4"};
        boolean[] poison = {false, true, false, false};

        List<String> log = new ArrayList<>();
        int processed = 0, dead = 0;
        for (int i = 0; i < msgs.length; i++) {
            if (!poison[i]) {
                log.add(ev("action", msgs[i], "ACK", "استُهلكت ونجحت ✓"));
                processed++;
                continue;
            }
            // رسالة سامّة
            for (int r = 1; r <= maxRetries; r++)
                log.add(ev("warn", msgs[i], "RETRY", "محاولة " + r + "/" + maxRetries + " — فشل المعالجة"));
            if (dlx) {
                log.add(ev("info", msgs[i], "→ DLX", "بعد استنفاد المحاولات: نُقلت إلى Dead-Letter Queue للفحص لاحقاً"));
                dead++;
            } else {
                log.add(ev("error", msgs[i], "BLOCK/LOOP",
                    "بلا DLX: تُعاد للطابور بلا نهاية فتحجب ما بعدها (head-of-line) أو تُفقَد بصمت ✗"));
                log.add(ev("error", "بقيّة الطابور", "STUCK", "msg#3 و msg#4 لم تُعالَج بسبب الرسالة العالقة"));
                respondDlx(ex, dlx, maxRetries, log, processed, dead, false);
                return;
            }
        }
        respondDlx(ex, dlx, maxRetries, log, processed, dead, true);
    }

    private void respondDlx(HttpExchange ex, boolean dlx, int retries, List<String> log,
                            int processed, int dead, boolean drained) throws IOException {
        String note = dlx
            ? "DLX: بعد عدد محاولات محدّد تُعزَل الرسالة السامّة في طابور موتى، فيستمرّ الطابور الرئيسي بلا توقّف وتبقى الرسالة للتحليل."
            : "طابور عادي: الرسالة السامّة تُعيد المحاولة بلا حدّ فتحجب الطابور أو تُفقَد — لا عزل ولا أثر.";
        respond(ex, "{\"mode\":\"" + (dlx ? "dlx" : "regular") + "\",\"retries\":" + retries
            + ",\"processed\":" + processed + ",\"dead\":" + dead + ",\"drained\":" + drained
            + ",\"log\":[" + String.join(",", log) + "],\"note\":\"" + esc(note) + "\"}");
    }

    // ═══════════════════════════════════════════════════════════════
    // 5) Sync (REST) vs Async (Message Queue)
    // ═══════════════════════════════════════════════════════════════
    private void handleComm(HttpExchange ex) throws IOException {
        Map<String,String> q = query(ex);
        boolean async = q.getOrDefault("mode", "sync").equalsIgnoreCase("async");
        boolean downstreamDown = Boolean.parseBoolean(q.getOrDefault("down", "false"));
        int[] lat = {120, 200, 80};   // كمون كل خدمة تابعة (ms)
        String[] svc = {"المخزون", "الدفع", "الشحن"};

        List<String> log = new ArrayList<>();
        int callerLatency, totalWork = lat[0] + lat[1] + lat[2];
        boolean callerOk;
        if (!async) {
            // متزامن: ينتظر سلسلة النداءات، ويفشل كاملاً لو سقطت خدمة
            int acc = 0;
            for (int i = 0; i < svc.length; i++) {
                if (downstreamDown && i == 1) {
                    log.add(ev("error", "REST → " + svc[i], "TIMEOUT", "الخدمة ساقطة — فشل الطلب كاملاً (اقتران زمني)"));
                    callerLatency = acc + 3000; callerOk = false;
                    respondComm(ex, async, callerLatency, totalWork, callerOk, log); return;
                }
                acc += lat[i];
                log.add(ev("action", "REST → " + svc[i], "200 OK", "انتظر " + lat[i] + "ms (المتصل محجوب)"));
            }
            callerLatency = acc; callerOk = true;
        } else {
            // لا متزامن: ينشر للطابور ويعود فوراً، والعمل يُعالَج بالخلفية
            callerLatency = 5;
            log.add(ev("action", "نشر إلى الطابور", "ACCEPTED", "المتصل يعود خلال ~5ms (لا انتظار)"));
            for (int i = 0; i < svc.length; i++) {
                if (downstreamDown && i == 1) {
                    log.add(ev("warn", "عامل → " + svc[i], "RETRY LATER", "الخدمة ساقطة — الرسالة تبقى بالطابور وتُعالَج عند العودة (لا تأثير على المتصل)"));
                    continue;
                }
                log.add(ev("action", "عامل → " + svc[i], "DONE", "عولجت بالخلفية (" + lat[i] + "ms)"));
            }
            callerOk = true;
        }
        respondComm(ex, async, callerLatency, totalWork, callerOk, log);
    }

    private void respondComm(HttpExchange ex, boolean async, int callerLatency, int totalWork,
                             boolean callerOk, List<String> log) throws IOException {
        String note = async
            ? "Async (MQ): المتصل يُرسل ويعود فوراً — اقتران أضعف ومرونة أعلى أمام الأعطال، لكن النتيجة نهائية لاحقاً (eventual)."
            : "Sync (REST): المتصل ينتظر النتيجة كاملة — أبسط وفوري، لكنه مقترن زمنياً: بطء أو سقوط أي خدمة يُسقط الطلب.";
        respond(ex, "{\"mode\":\"" + (async ? "async" : "sync") + "\",\"callerLatencyMs\":" + callerLatency
            + ",\"totalWorkMs\":" + totalWork + ",\"callerOk\":" + callerOk
            + ",\"log\":[" + String.join(",", log) + "],\"note\":\"" + esc(note) + "\"}");
    }

    // ═══════════════════════════════════════════════════════════════
    // 6) Strong vs Eventual Consistency
    // ═══════════════════════════════════════════════════════════════
    private void handleConsistency(HttpExchange ex) throws IOException {
        Map<String,String> q = query(ex);
        boolean strong = !q.getOrDefault("mode", "strong").equalsIgnoreCase("eventual");

        List<String> log = new ArrayList<>();
        // الحالة الابتدائية: الجميع = v1
        if (strong) {
            log.add(ev("action", "كتابة v2 إلى القائد", "PENDING", "تنتظر تأكيد الأغلبية قبل الإقرار"));
            log.add(ev("action", "نسخ متزامن", "QUORUM", "النسخ إلى أغلبية (2/3) قبل الردّ للعميل"));
            log.add(ev("read", "قراءة فور الكتابة", "v2", "كل القرّاء يرون v2 فوراً — لا قراءة قديمة"));
            log.add(ev("info", "المقايضة", "LATENCY", "اتساق قوي لكن كمون كتابة أعلى وإتاحة أقل عند الانقسام (CP)"));
        } else {
            log.add(ev("action", "كتابة v2 إلى عقدة واحدة", "ACK", "تُقِرّ فوراً دون انتظار البقيّة"));
            log.add(ev("read", "قراءة من تابع (t+0ms)", "v1", "قراءة قديمة! النسخ لم يصل بعد (نافذة عدم اتساق)"));
            log.add(ev("read", "قراءة من تابع (t+30ms)", "v1", "ما زالت قديمة أثناء الانتشار"));
            log.add(ev("action", "نسخ غير متزامن", "GOSSIP", "ينتشر التحديث للنسخ في الخلفية"));
            log.add(ev("read", "قراءة من تابع (t+80ms)", "v2", "تقاربت النسخ — أصبح الجميع v2"));
            log.add(ev("info", "المقايضة", "AVAILABILITY", "كمون منخفض وإتاحة عالية، لكن قراءات قديمة مؤقتة (AP)"));
        }
        String note = strong
            ? "Strong: بعد نجاح الكتابة، كل قراءة ترى أحدث قيمة (linearizable) — مثل Raft/etcd. الثمن: كمون وإتاحة."
            : "Eventual: تُقِرّ الكتابة فوراً وتتقارب النسخ لاحقاً — مثل Dynamo/Cassandra. الثمن: قراءات قديمة مؤقتة.";
        respond(ex, "{\"mode\":\"" + (strong ? "strong" : "eventual") + "\",\"log\":["
            + String.join(",", log) + "],\"note\":\"" + esc(note) + "\"}");
    }

    // ── مساعدات ─────────────────────────────────────────────────────
    /** عنصر سجلّ موحّد: {type, step, status, detail} */
    private String ev(String type, String step, String status, String detail) {
        return "{\"type\":\"" + esc(type) + "\",\"step\":\"" + esc(step)
            + "\",\"status\":\"" + esc(status) + "\",\"detail\":\"" + esc(detail) + "\"}";
    }

    private Map<String,String> query(HttpExchange ex) {
        Map<String,String> m = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null) return m;
        for (String p : raw.split("&")) {
            int i = p.indexOf('=');
            if (i > 0) m.put(dec(p.substring(0, i)), dec(p.substring(i + 1)));
        }
        return m;
    }
    private int intParam(Map<String,String> q, String k, int def) {
        try { return q.containsKey(k) ? Integer.parseInt(q.get(k).trim()) : def; }
        catch (Exception e) { return def; }
    }
    private String dec(String s) { return URLDecoder.decode(s, StandardCharsets.UTF_8); }
    private String esc(String s) { return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " "); }

    private void respond(HttpExchange ex, String json) throws IOException {
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(200, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }
}
