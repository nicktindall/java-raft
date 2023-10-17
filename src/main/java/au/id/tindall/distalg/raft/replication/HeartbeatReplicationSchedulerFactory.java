package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public class HeartbeatReplicationSchedulerFactory<I extends Serializable> implements ReplicationSchedulerFactory<I> {

    private final long maxDelayBetweenMessagesInMilliseconds;

    public HeartbeatReplicationSchedulerFactory(long maxDelayBetweenMessagesInMilliseconds) {
        this.maxDelayBetweenMessagesInMilliseconds = maxDelayBetweenMessagesInMilliseconds;
    }

    @Override
    public ReplicationScheduler create(I serverId) {
        return new HeartbeatReplicationScheduler(maxDelayBetweenMessagesInMilliseconds, System::currentTimeMillis);
    }
}
