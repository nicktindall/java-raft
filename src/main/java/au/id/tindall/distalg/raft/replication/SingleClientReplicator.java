package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class SingleClientReplicator<ID extends Serializable> {

    private final ReplicationScheduler replicationScheduler;
    private StateReplicator<ID> stateReplicator;

    public SingleClientReplicator(ID followerId, ReplicationScheduler replicationScheduler, LogReplicatorFactory<ID> logReplicatorFactory) {
        this.replicationScheduler = replicationScheduler;
        this.stateReplicator = logReplicatorFactory.createLogReplicator(followerId);
        replicationScheduler.setSendAppendEntriesRequest(this::sendNexReplicationMessage);
    }

    private void sendNexReplicationMessage() {
        stateReplicator.sendNextReplicationMessage();
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

    public int getMatchIndex() {
        return stateReplicator.getMatchIndex();
    }

    public int getNextIndex() {
        return stateReplicator.getNextIndex();
    }

    public void logFailedResponse(Integer followerLastLogIndex) {
        stateReplicator.logFailedResponse(followerLastLogIndex);
    }
}
