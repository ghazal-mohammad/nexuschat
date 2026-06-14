package nexus;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface RaftNode extends Remote {
    VoteResponse requestVote(long term, int candidateId, int lastLogIndex, long lastLogTerm) throws RemoteException;
    AppendResponse appendEntries(long term, int leaderId, List<LogEntry> entries, int leaderCommit) throws RemoteException;
}