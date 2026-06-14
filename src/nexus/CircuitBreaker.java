package nexus;

public class CircuitBreaker {
    public enum State { CLOSED, OPEN, HALF_OPEN }
    private State state = State.CLOSED;
    private int failures = 0;
    private long openedAt = 0;
    private final int threshold;
    private final long cooldownMs;

    public CircuitBreaker(int threshold, long cooldownMs) {
        this.threshold = threshold;
        this.cooldownMs = cooldownMs;
    }

    public synchronized boolean allow() {
        if (state == State.OPEN && System.currentTimeMillis() - openedAt >= cooldownMs)
            state = State.HALF_OPEN;          // بعد التبريد: جرّب طلباً واحداً
        return state != State.OPEN;
    }

    public synchronized void success() { failures = 0; state = State.CLOSED; }

    public synchronized void failure() {
        failures++;
        if (failures >= threshold) { state = State.OPEN; openedAt = System.currentTimeMillis(); }
    }

    public synchronized State state() { return state; }
}