package au.id.tindall.distalg.raft.replication;

import java.util.function.Supplier;

public class HeartbeatReplicationScheduler implements ReplicationScheduler {

    private final long maxDelayBetweenMessagesInMilliseconds;
    private final Supplier<Long> currentTimeProvider;
    private volatile boolean running;
    private long lastReplicationTime;

    public HeartbeatReplicationScheduler(long maxDelayBetweenMessagesInMilliseconds, Supplier<Long> currentTimeProvider) {
        this.maxDelayBetweenMessagesInMilliseconds = maxDelayBetweenMessagesInMilliseconds;
        this.currentTimeProvider = currentTimeProvider;
        running = false;
    }

    @Override
    public void start() {
        if (running) {
            throw new IllegalStateException("Attempted to start replication scheduler twice");
        }
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            throw new IllegalStateException("Attempted to stop non-running replication scheduler");
        }
        running = false;
    }

    @Override
    public void replicated() {
        lastReplicationTime = currentTimeProvider.get();
    }

    @Override
    public boolean replicationIsDue() {
        return running && currentTimeProvider.get() - lastReplicationTime > maxDelayBetweenMessagesInMilliseconds;
    }
}
