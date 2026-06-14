package nexus;
import java.io.Serializable;

public class AppendResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    public final long term;
    public final boolean success;
    public AppendResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }
}