package nexus;
import java.io.Serializable;

public class VoteResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    public final long term;
    public final boolean voteGranted;
    public VoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}