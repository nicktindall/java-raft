package au.id.tindall.distalg.raft.replication;

import org.apache.logging.log4j.Logger;

import java.io.Serializable;

import static org.apache.logging.log4j.status.StatusLogger.getLogger;

public class SingleClientReplicator<ID extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private final ID followerId;
    private final ReplicationScheduler replicationScheduler;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final SnapshotReplicatorFactory<ID> snapshotReplicatorFactory;
    private StateReplicator<ID> stateReplicator;

    public SingleClientReplicator(ID followerId, ReplicationScheduler replicationScheduler,
                                  LogReplicatorFactory<ID> logReplicatorFactory,
                                  SnapshotReplicatorFactory<ID> snapshotReplicatorFactory) {
        this.followerId = followerId;
        this.replicationScheduler = replicationScheduler;
        this.logReplicatorFactory = logReplicatorFactory;
        this.snapshotReplicatorFactory = snapshotReplicatorFactory;
        this.stateReplicator = logReplicatorFactory.createLogReplicator(followerId);
        replicationScheduler.setSendAppendEntriesRequest(this::sendNexReplicationMessage);
    }

    private synchronized void sendNexReplicationMessage() {
        switch (stateReplicator.sendNextReplicationMessage()) {
            case SwitchToLogReplication:
                stateReplicator = logReplicatorFactory.createLogReplicator(followerId);
                sendNexReplicationMessage();
                return;
            case SwitchToSnapshotReplication:
                stateReplicator = snapshotReplicatorFactory.createSnapshotReplicator(followerId);
                sendNexReplicationMessage();
                return;
            default:
                // Do nothing
        }
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

    public void logSuccessResponse(int lastAppendedIndex) {
        stateReplicator.logSuccessResponse(lastAppendedIndex);
    }

    public void logSuccessSnapshotResponse(int lastIndex, int lastOffset) {
        stateReplicator.logSuccessSnapshotResponse(lastIndex, lastOffset);
    }

    public int getMatchIndex() {
        return stateReplicator.getMatchIndex();
    }

    public int getNextIndex() {
        return stateReplicator.getNextIndex();
    }

    public void logFailedResponse(Integer earliestPossibleMatchIndex) {
        stateReplicator.logFailedResponse(earliestPossibleMatchIndex);
    }
}
