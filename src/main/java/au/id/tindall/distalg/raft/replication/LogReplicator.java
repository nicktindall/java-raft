package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.EntryStatus;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.apache.logging.log4j.LogManager.getLogger;

public class LogReplicator<ID extends Serializable> implements StateReplicator<ID> {

    private static final Logger LOGGER = getLogger();

    private final Log log;
    private final Term term;
    private final Cluster<ID> cluster;
    private final ID followerId;
    private final int maxBatchSize;
    private volatile int matchIndex;
    private volatile int nextIndex;

    public LogReplicator(Log log, Term term, Cluster<ID> cluster, ID followerId, int maxBatchSize, int nextIndex) {
        this.log = log;
        this.term = term;
        this.cluster = cluster;
        this.followerId = followerId;
        this.maxBatchSize = maxBatchSize;
        this.matchIndex = 0;
        this.nextIndex = nextIndex;
    }

    @Override
    public ReplicationResult sendNextReplicationMessage() {
        int nextIndexToSend = nextIndex;    // This can change, take a copy
        int prevLogIndex = nextIndexToSend - 1;
        if (log.hasEntry(nextIndexToSend) == EntryStatus.BeforeStart) {
            LOGGER.debug("Switching to snapshot replication. follower: {}, nextIndex: {}, matchIndex: {}, prevIndex: {}",
                    followerId, nextIndexToSend, matchIndex, log.getPrevIndex());
            return ReplicationResult.SwitchToSnapshotReplication;
        }
        try {
            Optional<Term> prevLogTerm = getTermAtIndex(log, prevLogIndex);
            List<LogEntry> entriesToReplicate = getEntriesToReplicate(log, nextIndexToSend);
            cluster.sendAppendEntriesRequest(term, followerId,
                    prevLogIndex, prevLogTerm, entriesToReplicate,
                    log.getCommitIndex());
            return ReplicationResult.StayInCurrentMode;
        } catch (IndexOutOfBoundsException e) {
            LOGGER.debug("Concurrent truncation caused switch to snapshot replication. follower: {}, nextIndex: {}, matchIndex: {}, prevIndex: {}",
                    followerId, nextIndexToSend, matchIndex, log.getPrevIndex());
            return ReplicationResult.SwitchToSnapshotReplication;
        }
    }

    private Optional<Term> getTermAtIndex(Log log, int index) {
        if (index == 0) {
            return Optional.empty();
        }
        final EntryStatus entryStatus = log.hasEntry(index);
        if (entryStatus == EntryStatus.Present) {
            return Optional.of(log.getEntry(index).getTerm());
        }
        if (entryStatus == EntryStatus.BeforeStart && index == log.getPrevIndex()) {
            return Optional.of(log.getPrevTerm());
        }
        throw new IndexOutOfBoundsException("Can't get term for index " + index + ", entryStatus=" + entryStatus);
    }

    @Override
    public synchronized void logSuccessResponse(int lastAppendedIndex) {
        nextIndex = Math.max(lastAppendedIndex + 1, nextIndex);
        matchIndex = Math.max(lastAppendedIndex, matchIndex);
    }

    @Override
    public synchronized void logFailedResponse(Integer earliestPossibleMatchIndex) {
        LOGGER.debug("Got failed response from {}, earliest possible match index: {}", followerId, earliestPossibleMatchIndex);
        if (earliestPossibleMatchIndex != null) {
            nextIndex = earliestPossibleMatchIndex;
        }
    }

    @Override
    public void logSuccessSnapshotResponse(int lastIndex, int lastOffset) {
        // Do nothing
    }

    @Override
    public int getMatchIndex() {
        return matchIndex;
    }

    @Override
    public int getNextIndex() {
        return nextIndex;
    }

    private List<LogEntry> getEntriesToReplicate(Log log, int nextIndex) {
        if (log.hasEntry(nextIndex) == EntryStatus.Present) {
            return log.getEntries(nextIndex, maxBatchSize);
        } else {
            return emptyList();
        }
    }

    @Override
    public String toString() {
        return "LogReplicator{" +
                "followerId=" + followerId +
                ", matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                '}';
    }
}
