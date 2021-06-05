package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;

public class LogReplicator<ID extends Serializable> {

    private final Log log;
    private final Term term;
    private final Cluster<ID> cluster;
    private final ID followerId;
    private final int maxBatchSize;
    private final ReplicationScheduler replicationScheduler;
    private volatile int matchIndex;
    private volatile int nextIndex;

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
        int nextIndexToSend = nextIndex;    // This can change, take a copy
        int prevLogIndex = nextIndexToSend - 1;
        Optional<Term> prevLogTerm = getTermAtIndex(log, prevLogIndex);
        List<LogEntry> entriesToReplicate = getEntriesToReplicate(log, nextIndexToSend);
        cluster.sendAppendEntriesRequest(term, followerId,
                prevLogIndex, prevLogTerm, entriesToReplicate,
                log.getCommitIndex());
    }

    private Optional<Term> getTermAtIndex(Log log, int index) {
        return index > 0 && log.hasEntry(index)
                ? Optional.of(log.getEntry(index).getTerm())
                : Optional.empty();
    }

    public synchronized void logSuccessResponse(int lastAppendedIndex) {
        nextIndex = Math.max(lastAppendedIndex + 1, nextIndex);
        matchIndex = Math.max(lastAppendedIndex, matchIndex);
    }

    public synchronized void logFailedResponse(Integer followerLastLogIndex) {
        int highestPossibleIndex = nextIndex - 1;
        if (followerLastLogIndex != null) {
            highestPossibleIndex = min(highestPossibleIndex, followerLastLogIndex + 1);
        }
        nextIndex = max(highestPossibleIndex, 1);
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
