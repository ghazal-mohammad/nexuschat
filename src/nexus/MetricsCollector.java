package nexus;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * ============================================================
 *  MetricsCollector — قياس الأداء الحقيقي
 *
 *  يُتيح إجابة عملية على PACELC:
 *  "ما تأثير الـ partition على الـ latency؟"
 *  → نقيس قبل وأثناء وبعد ونُظهر الأرقام.
 * ============================================================
 */
public class MetricsCollector {

    private static final int HISTORY_SECONDS = 60;

    // [timestamp_ms, latency_ms, nodeId]
    private final Deque<long[]>  samples = new ConcurrentLinkedDeque<>();
    private final Deque<int[]>   history = new ConcurrentLinkedDeque<>(); // [second, rps, avgMs]

    private final AtomicLong windowCount   = new AtomicLong(0);
    private final AtomicLong windowLatency = new AtomicLong(0);
    private volatile long    windowStart   = System.currentTimeMillis();

    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalLatency  = new AtomicLong(0);

    // ── تسجيل قياس ────────────────────────────────────────
    public void record(int nodeId, long latencyMs) {
        long now = System.currentTimeMillis();
        samples.addLast(new long[]{now, latencyMs, nodeId});
        while (samples.size() > 2000) samples.pollFirst();

        windowCount.incrementAndGet();
        windowLatency.addAndGet(latencyMs);
        totalRequests.incrementAndGet();
        totalLatency.addAndGet(latencyMs);

        // طَيّ النافذة كل ثانية
        if (now - windowStart >= 1000) {
            long cnt = windowCount.getAndSet(0);
            long lat = windowLatency.getAndSet(0);
            int avg   = cnt > 0 ? (int)(lat / cnt) : 0;
            history.addLast(new int[]{(int)(now / 1000), (int)cnt, avg});
            if (history.size() > HISTORY_SECONDS) history.pollFirst();
            windowStart = now;
        }
    }

    // ── قياسات لحظية ──────────────────────────────────────
    public double getThroughput() {
        long cutoff = System.currentTimeMillis() - 1000;
        long count = 0;
        for (long[] s : samples) if (s[0] >= cutoff) count++;
        return count;
    }

    public long getP50()  { return percentile(50); }
    public long getP95()  { return percentile(95); }
    public long getP99()  { return percentile(99); }

    private long percentile(int p) {
        List<Long> sorted = new ArrayList<>();
        for (long[] s : samples) sorted.add(s[1]);
        if (sorted.isEmpty()) return 0;
        Collections.sort(sorted);
        int idx = Math.min((int)(sorted.size() * p / 100.0), sorted.size() - 1);
        return sorted.get(idx);
    }

    public long getTotalRequests() { return totalRequests.get(); }

    public double getOverallAvg() {
        long t = totalRequests.get();
        return t == 0 ? 0.0 : (double) totalLatency.get() / t;
    }

    // ── JSON ──────────────────────────────────────────────
    public String snapshotJson() {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"rps\":").append(String.format("%.1f", getThroughput()));
        sb.append(",\"p50\":").append(getP50());
        sb.append(",\"p95\":").append(getP95());
        sb.append(",\"p99\":").append(getP99());
        sb.append(",\"avg\":").append(String.format("%.1f", getOverallAvg()));
        sb.append(",\"total\":").append(getTotalRequests());
        sb.append(",\"history\":[");
        boolean first = true;
        for (int[] h : new ArrayList<>(history)) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{\"t\":").append(h[0])
              .append(",\"rps\":").append(h[1])
              .append(",\"avg\":").append(h[2]).append("}");
        }
        sb.append("]}");
        return sb.toString();
    }
}
