package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class SingleClientReplicator<I extends Serializable> {

    private final ReplicationScheduler replicationScheduler;
    private final LogReplicatorFactory<I> logReplicatorFactory;
    private final SnapshotReplicatorFactory<I> snapshotReplicatorFactory;
    private final ReplicationState<I> replicationState;
    private StateReplicator stateReplicator;

    public SingleClientReplicator(ReplicationScheduler replicationScheduler,
                                  LogReplicatorFactory<I> logReplicatorFactory,
                                  SnapshotReplicatorFactory<I> snapshotReplicatorFactory,
                                  ReplicationState<I> replicationState) {
        this.replicationScheduler = replicationScheduler;
        this.logReplicatorFactory = logReplicatorFactory;
        this.snapshotReplicatorFactory = snapshotReplicatorFactory;
        this.stateReplicator = logReplicatorFactory.createLogReplicator(replicationState);
        this.replicationState = replicationState;
    }

    private void sendNextReplicationMessage(boolean force) {
        final StateReplicator.ReplicationResult replicationResult = stateReplicator.sendNextReplicationMessage(force);
        switch (replicationResult) {
            case SWITCH_TO_LOG_REPLICATION:
                stateReplicator = logReplicatorFactory.createLogReplicator(replicationState);
                sendNextReplicationMessage(force);
                break;
            case SWITCH_TO_SNAPSHOT_REPLICATION:
                stateReplicator = snapshotReplicatorFactory.createSnapshotReplicator(replicationState);
                sendNextReplicationMessage(force);
                break;
            case SUCCESS:
                replicationScheduler.replicated();
                // fall through
            case SKIPPED:
                break;
            default:
                throw new IllegalStateException("Unexpected ReplicationResult " + replicationResult);
        }
    }

    public boolean replicateIfDue() {
        if (replicationScheduler.replicationIsDue()) {
            sendNextReplicationMessage(true);
            return true;
        }
        return false;
    }

    public void start() {
        replicationScheduler.start();
    }

    public void stop() {
        replicationScheduler.stop();
    }

    public void replicate() {
        sendNextReplicationMessage(false);
    }

    public void logSuccessResponse(int lastAppendedIndex) {
        replicationState.logSuccessResponse(lastAppendedIndex);
    }

    public void logSuccessSnapshotResponse(int lastIndex, int lastOffset) {
        stateReplicator.logSuccessSnapshotResponse(lastIndex, lastOffset);
    }

    public int getMatchIndex() {
        return replicationState.getMatchIndex();
    }

    public int getNextIndex() {
        return replicationState.getNextIndex();
    }

    public void logFailedResponse(Integer earliestPossibleMatchIndex) {
        replicationState.logFailedResponse(earliestPossibleMatchIndex);
    }
}
