package nexus;
import java.io.Serializable;

public class LogEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    public final long term;
    public final String requestId;     // مفتاح Idempotency
    public final String command;
    public LogEntry(long term, String requestId, String command) {
        this.term = term;
        this.requestId = requestId;
        this.command = command;
    }
    @Override public String toString() { return "(t" + term + ":" + command + ")"; }
}