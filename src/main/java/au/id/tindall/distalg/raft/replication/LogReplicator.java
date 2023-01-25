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

public class LogReplicator<ID extends Serializable> implements StateReplicator {

    private static final Logger LOGGER = getLogger();

    private final Log log;
    private final Term term;
    private final Cluster<ID> cluster;
    private final int maxBatchSize;
    private final ReplicationState<ID> replicationState;

    public LogReplicator(Log log, Term term, Cluster<ID> cluster, int maxBatchSize, ReplicationState<ID> replicationState) {
        this.log = log;
        this.term = term;
        this.cluster = cluster;
        this.maxBatchSize = maxBatchSize;
        this.replicationState = replicationState;
    }

    @Override
    public ReplicationResult sendNextReplicationMessage() {
        int nextIndexToSend = replicationState.getNextIndex();    // This can change, take a copy
        int prevLogIndex = nextIndexToSend - 1;
        if (log.hasEntry(nextIndexToSend) == EntryStatus.BEFORE_START) {
            LOGGER.debug("Switching to snapshot replication. follower: {}, nextIndex: {}, matchIndex: {}, prevIndex: {}",
                    replicationState.getFollowerId(), nextIndexToSend, replicationState.getMatchIndex(), log.getPrevIndex());
            return ReplicationResult.SWITCH_TO_SNAPSHOT_REPLICATION;
        }
        try {
            Optional<Term> prevLogTerm = getTermAtIndex(log, prevLogIndex);
            List<LogEntry> entriesToReplicate = getEntriesToReplicate(log, nextIndexToSend);
            cluster.sendAppendEntriesRequest(term, replicationState.getFollowerId(),
                    prevLogIndex, prevLogTerm, entriesToReplicate,
                    log.getCommitIndex());
            return ReplicationResult.STAY_IN_CURRENT_MODE;
        } catch (IndexOutOfBoundsException e) {
            LOGGER.debug("Concurrent truncation caused switch to snapshot replication. follower: {}, nextIndex: {}, matchIndex: {}, prevIndex: {}",
                    replicationState.getFollowerId(), nextIndexToSend, replicationState.getMatchIndex(), log.getPrevIndex());
            return ReplicationResult.SWITCH_TO_SNAPSHOT_REPLICATION;
        }
    }

    private Optional<Term> getTermAtIndex(Log log, int index) {
        if (index == 0) {
            return Optional.empty();
        }
        final EntryStatus entryStatus = log.hasEntry(index);
        if (entryStatus == EntryStatus.PRESENT) {
            return Optional.of(log.getEntry(index).getTerm());
        }
        if (entryStatus == EntryStatus.BEFORE_START && index == log.getPrevIndex()) {
            return Optional.of(log.getPrevTerm());
        }
        throw new IndexOutOfBoundsException("Can't get term for index " + index + ", entryStatus=" + entryStatus);
    }

    @Override
    public void logSuccessSnapshotResponse(int lastIndex, int lastOffset) {
        // Do nothing
    }

    private List<LogEntry> getEntriesToReplicate(Log log, int nextIndex) {
        if (log.hasEntry(nextIndex) == EntryStatus.PRESENT) {
            return log.getEntries(nextIndex, maxBatchSize);
        } else {
            return emptyList();
        }
    }

    @Override
    public String toString() {
        return "LogReplicator{" +
                ", replicationState=" + replicationState +
                '}';
    }
}
