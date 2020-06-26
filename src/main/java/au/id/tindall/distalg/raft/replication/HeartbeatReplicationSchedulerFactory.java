package au.id.tindall.distalg.raft.replication;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class HeartbeatReplicationSchedulerFactory implements ReplicationSchedulerFactory {

    private final long maxDelayBetweenMessagesInMilliseconds;

    public HeartbeatReplicationSchedulerFactory(long maxDelayBetweenMessagesInMilliseconds) {
        this.maxDelayBetweenMessagesInMilliseconds = maxDelayBetweenMessagesInMilliseconds;
    }

    @Override
    public ReplicationScheduler create() {
        return new HeartbeatReplicationScheduler(maxDelayBetweenMessagesInMilliseconds, newSingleThreadExecutor());
    }
}
