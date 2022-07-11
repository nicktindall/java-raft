package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class HeartbeatReplicationSchedulerFactory<ID extends Serializable> implements ReplicationSchedulerFactory<ID> {

    private final long maxDelayBetweenMessagesInMilliseconds;

    public HeartbeatReplicationSchedulerFactory(long maxDelayBetweenMessagesInMilliseconds) {
        this.maxDelayBetweenMessagesInMilliseconds = maxDelayBetweenMessagesInMilliseconds;
    }

    @Override
    public ReplicationScheduler create(ID serverId) {
        return new HeartbeatReplicationScheduler<>(serverId, maxDelayBetweenMessagesInMilliseconds, newSingleThreadExecutor());
    }
}
