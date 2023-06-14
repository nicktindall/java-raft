package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

import static au.id.tindall.distalg.raft.threading.NamedThreadFactory.forSingleThread;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class HeartbeatReplicationSchedulerFactory<I extends Serializable> implements ReplicationSchedulerFactory<I> {

    private final long maxDelayBetweenMessagesInMilliseconds;

    public HeartbeatReplicationSchedulerFactory(long maxDelayBetweenMessagesInMilliseconds) {
        this.maxDelayBetweenMessagesInMilliseconds = maxDelayBetweenMessagesInMilliseconds;
    }

    @Override
    public ReplicationScheduler create(I serverId) {
        return new HeartbeatReplicationScheduler<>(serverId, maxDelayBetweenMessagesInMilliseconds, newSingleThreadExecutor(forSingleThread("replicator-" + serverId)));
    }
}
