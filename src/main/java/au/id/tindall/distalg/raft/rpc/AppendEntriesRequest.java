package au.id.tindall.distalg.raft.rpc;

import static java.util.List.copyOf;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import au.id.tindall.distalg.raft.log.LogEntry;
import au.id.tindall.distalg.raft.log.Term;

public class AppendEntriesRequest<ID extends Serializable> extends BroadcastMessage<ID> {

    private final ID leaderId;
    private final int prevLogIndex;
    private final Term prevLogTerm;
    private final List<LogEntry> entries;
    private final int leaderCommit;

    public AppendEntriesRequest(Term term, ID leaderId, int prevLogIndex, Optional<Term> prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        super(term, leaderId);
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
                ", entries=" + entries +
                ", leaderCommit=" + leaderCommit +
                "} " + super.toString();
    }
}
