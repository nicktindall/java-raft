package au.id.tindall.distalg.raft.replication;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static au.id.tindall.distalg.raft.util.ExecutorUtil.shutdownAndAwaitTermination;

public class HeartbeatReplicationScheduler<ID extends Serializable> implements ReplicationScheduler {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int WAIT_FOR_TERMINATE_TIMEOUT_MILLISECONDS = 1_500;

    private final ID serverId;
    private final long maxDelayBetweenMessagesInMilliseconds;
    private final ExecutorService executorService;
    private final AtomicBoolean replicationScheduled;
    private volatile boolean running;
    private Supplier<Boolean> sendAppendEntriesRequest;

    public HeartbeatReplicationScheduler(ID serverId, long maxDelayBetweenMessagesInMilliseconds, ExecutorService executorService) {
        this.serverId = serverId;
        this.maxDelayBetweenMessagesInMilliseconds = maxDelayBetweenMessagesInMilliseconds;
        this.executorService = executorService;
        replicationScheduled = new AtomicBoolean(false);
        running = false;
    }

    @Override
    public void setSendAppendEntriesRequest(Supplier<Boolean> sendAppendEntriesRequest) {
        this.sendAppendEntriesRequest = sendAppendEntriesRequest;
    }

    @Override
    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("Attempted to start replication scheduler twice");
        }
        running = true;
        executorService.submit(this::run);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            throw new IllegalStateException("Attempted to stop non-running replication scheduler");
        }
        running = false;
        synchronized (replicationScheduled) {
            replicationScheduled.notifyAll();
        }
        shutdownAndAwaitTermination(executorService, WAIT_FOR_TERMINATE_TIMEOUT_MILLISECONDS);
    }

    private Void run() {
        try (CloseableThreadContext.Instance ctc = CloseableThreadContext.put("serverId", serverId.toString())) {
            while (running) {
                if (replicationScheduled.getAndSet(false)) {
                    if (!sendAppendEntriesRequest.get()) {
                        replicationScheduled.set(true);
                    }
                } else {
                    synchronized (replicationScheduled) {
                        replicationScheduled.wait(maxDelayBetweenMessagesInMilliseconds);
                    }
                    replicationScheduled.set(true);
                }
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Replication thread failed!", ex);
        } catch (InterruptedException e) {
            LOGGER.debug("Replication thread interrupted!");
            Thread.currentThread().interrupt();
        }
        return null;
    }

    @Override
    public void replicate() {
        this.replicationScheduled.set(true);
        synchronized (this.replicationScheduled) {
            this.replicationScheduled.notifyAll();
        }
    }
}
