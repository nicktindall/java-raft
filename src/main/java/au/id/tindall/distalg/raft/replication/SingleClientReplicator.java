package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class SingleClientReplicator<ID extends Serializable> {

    private final ReplicationScheduler replicationScheduler;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final SnapshotReplicatorFactory<ID> snapshotReplicatorFactory;
    private final ReplicationState<ID> replicationState;
    private StateReplicator stateReplicator;

    public SingleClientReplicator(ReplicationScheduler replicationScheduler,
                                  LogReplicatorFactory<ID> logReplicatorFactory,
                                  SnapshotReplicatorFactory<ID> snapshotReplicatorFactory,
                                  ReplicationState<ID> replicationState) {
        this.replicationScheduler = replicationScheduler;
        this.logReplicatorFactory = logReplicatorFactory;
        this.snapshotReplicatorFactory = snapshotReplicatorFactory;
        this.stateReplicator = logReplicatorFactory.createLogReplicator(replicationState);
        this.replicationState = replicationState;
        replicationScheduler.setSendAppendEntriesRequest(this::sendNexReplicationMessage);
    }

    private synchronized void sendNexReplicationMessage() {
        switch (stateReplicator.sendNextReplicationMessage()) {
            case SwitchToLogReplication:
                stateReplicator = logReplicatorFactory.createLogReplicator(replicationState);
                sendNexReplicationMessage();
                return;
            case SwitchToSnapshotReplication:
                stateReplicator = snapshotReplicatorFactory.createSnapshotReplicator(replicationState);
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
