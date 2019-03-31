package au.id.tindall.distalg.raft.replication;

import static java.lang.Math.max;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;

public class LogReplicator<ID extends Serializable> {

    private final ID serverId;
    private final Cluster<ID> cluster;
    private final ID followerId;
    private int matchIndex;
    private int nextIndex;

    public LogReplicator(ID serverId, Cluster<ID> cluster, ID followerId, int nextIndex) {
        this.serverId = serverId;
        this.cluster = cluster;
        this.followerId = followerId;
        this.matchIndex = 0;
        this.nextIndex = nextIndex;
    }

    public void sendAppendEntriesRequest(Term currentTerm, Log log) {
        int prevLogIndex = nextIndex - 1;
        Optional<Term> prevLogTerm = getTermAtIndex(log, prevLogIndex);
        List<LogEntry> entriesToReplicate = getEntryToReplicate(log, nextIndex);
        cluster.send(new AppendEntriesRequest<>(currentTerm, serverId, followerId,
                prevLogIndex, prevLogTerm, entriesToReplicate,
                log.getCommitIndex()));
    }

    private Optional<Term> getTermAtIndex(Log log, int index) {
        return index > 0 && log.hasEntry(index)
                ? Optional.of(log.getEntry(index).getTerm())
                : Optional.empty();
    }

    public void logSuccessResponse(int lastAppendedIndex) {
        nextIndex = lastAppendedIndex + 1;
        matchIndex = lastAppendedIndex;
    }

    public void logFailedResponse() {
        nextIndex = max(nextIndex - 1, 1);
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    private List<LogEntry> getEntryToReplicate(Log log, int nextIndex) {
        if (log.hasEntry(nextIndex)) {
            return singletonList(log.getEntry(nextIndex));
        } else {
            return emptyList();
        }
    }
}
