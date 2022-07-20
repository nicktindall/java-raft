package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static java.util.List.copyOf;

public class AppendEntriesRequest<ID extends Serializable> extends UnicastMessage<ID> {

    private final ID leaderId;
    private final int prevLogIndex;
    private final Term prevLogTerm;
    private final List<LogEntry> entries;
    private final int leaderCommit;

    public AppendEntriesRequest(Term term, ID leaderId, ID destinationId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        super(term, leaderId, destinationId);
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm.orElse(null);
        this.entries = copyOf(entries);
        this.leaderCommit = leaderCommit;
    }

    public ID getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public Term getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
                "leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries.size()=" + entries.size() +
                ", leaderCommit=" + leaderCommit +
                "} " + super.toString();
    }
}
