package nexus;

import java.io.Serializable;
import java.util.*;

/**
 * ============================================================
 *  Vector Clock — ساعة المتجه
 *  موزعة-2: Happened-Before Relation + Causal Ordering
 *
 *  المفهوم:
 *   كل عقدة تحتفظ بمصفوفة [vc[0], vc[1], vc[2]...]
 *   vc[i] = عدد الأحداث التي رأتها العقدة i
 *
 *  قواعد التحديث:
 *   - عند حدث محلي:          vc[self]++
 *   - عند إرسال رسالة:       vc[self]++  ثم أرسل نسخة vc
 *   - عند استقبال رسالة:     vc[i] = max(vc[i], received[i]) لكل i، ثم vc[self]++
 *
 *  العلاقات:
 *   A → B  (A سبق B):    vc_A ≤ vc_B  و  vc_A ≠ vc_B
 *   A ∥ B  (متزامنان):   لا هذا ولا ذاك (تعارض محتمل!)
 *
 *  مثال في الدردشة:
 *   رسالة من أحمد: [1,0,0]
 *   رد  من  سارة: [1,1,0] → يعني سارة رأت رسالة أحمد أولاً (A → B)
 * ============================================================
 */
public class VectorClock implements Serializable, Comparable<VectorClock> {

    private final int[] clock;
    private final int   nodeId;
    private final int   clusterSize;

    public VectorClock(int nodeId, int clusterSize) {
        this.nodeId      = nodeId;
        this.clusterSize = clusterSize;
        this.clock       = new int[clusterSize];
    }

    /** نسخة للإرسال */
    private VectorClock(int nodeId, int[] clock) {
        this.nodeId      = nodeId;
        this.clusterSize = clock.length;
        this.clock       = Arrays.copyOf(clock, clock.length);
    }

    // ── أحداث محلية ──────────────────────────────────────────

    /** حدث محلي: vc[self]++ */
    public synchronized void tick() {
        clock[nodeId]++;
    }

    /** قبل الإرسال: tick ثم أعطِ نسخة */
    public synchronized VectorClock send() {
        tick();
        return snapshot();
    }

    /** عند الاستقبال: merge مع الوارد، ثم tick */
    public synchronized void receive(VectorClock incoming) {
        for (int i = 0; i < clusterSize; i++)
            clock[i] = Math.max(clock[i], incoming.clock[i]);
        clock[nodeId]++;
    }

    /** عند الاستقبال من مصفوفة خام (رسالة شبكة): merge ثم tick */
    public synchronized void receiveArray(int[] incoming) {
        int n = Math.min(clusterSize, incoming.length);
        for (int i = 0; i < n; i++)
            clock[i] = Math.max(clock[i], incoming[i]);
        clock[nodeId]++;
    }

    // ── مقارنات ───────────────────────────────────────────────

    /** A → B  (A سبق B causally) */
    public static boolean happenedBefore(VectorClock a, VectorClock b) {
        boolean strictlyLess = false;
        for (int i = 0; i < a.clusterSize; i++) {
            if (a.clock[i] > b.clock[i]) return false;
            if (a.clock[i] < b.clock[i]) strictlyLess = true;
        }
        return strictlyLess;
    }

    /** A ∥ B  (متزامنان / متعارضان) */
    public static boolean concurrent(VectorClock a, VectorClock b) {
        return !happenedBefore(a, b) && !happenedBefore(b, a) && !a.equals(b);
    }

    /** نص يصف العلاقة */
    public static String relation(VectorClock a, VectorClock b) {
        if (happenedBefore(a, b)) return "A → B  (A سبق B)";
        if (happenedBefore(b, a)) return "B → A  (B سبق A)";
        if (a.equals(b))          return "A = B  (متطابقان)";
        return "A ∥ B  (⚠ تعارض محتمل!)";
    }

    // ── مساعدات ──────────────────────────────────────────────

    public synchronized VectorClock snapshot() {
        return new VectorClock(nodeId, clock);
    }

    public synchronized int get(int i) { return clock[i]; }

    @Override
    public synchronized boolean equals(Object o) {
        if (!(o instanceof VectorClock other)) return false;
        return Arrays.equals(clock, other.clock);
    }

    @Override
    public int hashCode() { return Arrays.hashCode(clock); }

    @Override
    public int compareTo(VectorClock other) {
        if (happenedBefore(this, other)) return -1;
        if (happenedBefore(other, this)) return  1;
        return 0;
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < clusterSize; i++) {
            if (i > 0) sb.append(", ");
            sb.append("N").append(i + 1).append(":").append(clock[i]);
        }
        return sb.append("]").toString();
    }
}
