package nexus;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * ============================================================
 *  Bulkhead Pattern — عازل الموارد
 *  Session 06 / موزعة-1: Fault Tolerance
 *
 *  المفهوم:
 *   كل خدمة تعمل في "حوض" (pool) مستقل من الخيوط.
 *   إذا غرق حوض خدمة واحدة (مثل تحميل الصور)،
 *   لا تتأثر باقي الخدمات (مثل الدردشة أو الدفع).
 *
 *  مثال حقيقي:
 *   - chat-pool:    10 خيوط
 *   - upload-pool:   3 خيوط
 *   - metrics-pool:  2 خيوط
 *   إذا انهمرت طلبات التحميل → chat لا تتوقف أبداً
 * ============================================================
 */
public class Bulkhead {

    private final String            name;
    private final ExecutorService   pool;
    private final Semaphore         semaphore;
    private final AtomicInteger     rejected   = new AtomicInteger(0);
    private final AtomicInteger     executed   = new AtomicInteger(0);
    private final AtomicInteger     active     = new AtomicInteger(0);

    public Bulkhead(String name, int maxConcurrent, int queueCapacity) {
        this.name      = name;
        this.semaphore = new Semaphore(maxConcurrent);
        this.pool      = new ThreadPoolExecutor(
                maxConcurrent, maxConcurrent,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                r -> new Thread(r, "bulkhead-" + name + "-" + executed.get()),
                (r, executor) -> {
                    rejected.incrementAndGet();
                    System.out.printf("[Bulkhead:%s] ⛔ رُفض الطلب — الحوض ممتلئ%n", name);
                }
        );
    }

    /**
     * نفّذ العملية داخل الحوض
     * يرجع Future يمكن الانتظار عليه أو إلغاؤه
     */
    public <T> Future<T> submit(Callable<T> task) {
        return pool.submit(() -> {
            active.incrementAndGet();
            try {
                executed.incrementAndGet();
                return task.call();
            } finally {
                active.decrementAndGet();
            }
        });
    }

    /** تنفيذ مع Timeout */
    public <T> T executeWithTimeout(Callable<T> task, long timeoutMs) {
        try {
            Future<T> f = submit(task);
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            System.out.printf("[Bulkhead:%s] ⏰ انتهت المهلة (%dms)%n", name, timeoutMs);
            return null;
        } catch (Exception e) {
            System.out.printf("[Bulkhead:%s] ❌ خطأ: %s%n", name, e.getMessage());
            return null;
        }
    }

    public String getStats() {
        return String.format("Bulkhead[%s] active=%d executed=%d rejected=%d",
                name, active.get(), executed.get(), rejected.get());
    }

    public int getActive()   { return active.get();   }
    public int getRejected() { return rejected.get();  }
    public int getExecuted() { return executed.get();  }
    public String getName()  { return name;            }

    public void shutdown() { pool.shutdownNow(); }
}
