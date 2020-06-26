package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.max;
import static java.util.Collections.emptyList;

public class LogReplicator<ID extends Serializable> {

    private final Log log;
    private final Term term;
    private final Cluster<ID> cluster;
    private final ID followerId;
    private final int maxBatchSize;
    private int matchIndex;
    private int nextIndex;
    private final ReplicationScheduler replicationScheduler;

    public LogReplicator(Log log, Term term, Cluster<ID> cluster, ID followerId, int maxBatchSize, int nextIndex, ReplicationScheduler replicationScheduler) {
        this.log = log;
        this.term = term;
        this.cluster = cluster;
        this.followerId = followerId;
        this.maxBatchSize = maxBatchSize;
        this.replicationScheduler = replicationScheduler;
        this.matchIndex = 0;
        this.nextIndex = nextIndex;
        replicationScheduler.setSendAppendEntriesRequest(this::sendAppendEntriesRequest);
    }

    public void start() {
        replicationScheduler.start();
    }

    public void stop() {
        replicationScheduler.stop();
    }

    public void replicate() {
        replicationScheduler.replicate();
    }

    private void sendAppendEntriesRequest() {
        int prevLogIndex = nextIndex - 1;
        Optional<Term> prevLogTerm = getTermAtIndex(log, prevLogIndex);
        List<LogEntry> entriesToReplicate = getEntriesToReplicate(log, nextIndex);
        cluster.sendAppendEntriesRequest(term, followerId,
                prevLogIndex, prevLogTerm, entriesToReplicate,
                log.getCommitIndex());
    }

    private Optional<Term> getTermAtIndex(Log log, int index) {
        return index > 0 && log.hasEntry(index)
                ? Optional.of(log.getEntry(index).getTerm())
                : Optional.empty();
    }

    public void logSuccessResponse(int lastAppendedIndex) {
        nextIndex = Math.max(lastAppendedIndex + 1, nextIndex);
        matchIndex = Math.max(lastAppendedIndex, matchIndex);
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

    private List<LogEntry> getEntriesToReplicate(Log log, int nextIndex) {
        if (log.hasEntry(nextIndex)) {
            return log.getEntries(nextIndex, maxBatchSize);
        } else {
            return emptyList();
        }
    }
}
