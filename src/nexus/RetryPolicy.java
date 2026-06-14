package nexus;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * ============================================================
 *  RetryPolicy — Retry + Exponential Backoff + Jitter
 *  Session 06: Fault Tolerance
 *
 *  المفهوم:
 *   - نحاول تنفيذ العملية عدة مرات عند الفشل
 *   - نتضاعف وقت الانتظار بين كل محاولة (Exponential)
 *   - نضيف عشوائية (Jitter) لمنع "thundering herd"
 *     (كل العملاء يعيدون المحاولة في نفس اللحظة)
 *
 *  مثال Session06: تصل كالقهوة مع أحمد
 *   محاولة 1 → 1 ثانية → محاولة 2 → 2 ثانية → محاولة 3 → 4 ثانية...
 * ============================================================
 */
public class RetryPolicy {

    private final int    maxAttempts;
    private final long   initialDelayMs;
    private final double multiplier;
    private final long   maxDelayMs;
    private final boolean jitter;

    /** بناء بمعاملات افتراضية جيدة */
    public RetryPolicy(int maxAttempts, long initialDelayMs, double multiplier,
                       long maxDelayMs, boolean jitter) {
        this.maxAttempts    = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.multiplier     = multiplier;
        this.maxDelayMs     = maxDelayMs;
        this.jitter         = jitter;
    }

    /** بناء مبسّط: حاول N مرة، ابدأ بـ 200ms، ضاعف كل مرة */
    public static RetryPolicy defaultPolicy(int maxAttempts) {
        return new RetryPolicy(maxAttempts, 200, 2.0, 8000, true);
    }

    /**
     * نفّذ العملية مع إعادة المحاولة تلقائياً
     * @param operation  عملية قد تُلقي استثناء
     * @param label      اسم للطباعة في الـ logs
     */
    public <T> T execute(String label, Callable<T> operation) throws Exception {
        long delay = initialDelayMs;
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                T result = operation.call();
                if (attempt > 1)
                    System.out.printf("[Retry:%s] ✅ نجحت المحاولة %d%n", label, attempt);
                return result;
            } catch (Exception e) {
                lastException = e;
                if (attempt == maxAttempts) break;

                long wait = jitter ? (long)(delay * (0.5 + Math.random() * 0.5)) : delay;
                System.out.printf("[Retry:%s] ⏱ محاولة %d/%d فشلت (\"%s\") — انتظر %dms%n",
                        label, attempt, maxAttempts, e.getMessage(), wait);
                Thread.sleep(wait);
                delay = Math.min((long)(delay * multiplier), maxDelayMs);
            }
        }
        System.out.printf("[Retry:%s] ❌ فشلت جميع المحاولات (%d)%n", label, maxAttempts);
        throw lastException;
    }

    /** نسخة تُعيد null بدلاً من استثناء */
    public <T> T executeOrNull(String label, Callable<T> operation) {
        try { return execute(label, operation); }
        catch (Exception e) { return null; }
    }
}
