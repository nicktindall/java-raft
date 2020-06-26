package au.id.tindall.distalg.raft.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeartbeatReplicationScheduler implements ReplicationScheduler {

    private static final Logger LOGGER = LogManager.getLogger();

    private final long maxDelayBetweenMessagesInMilliseconds;
    private final ExecutorService executorService;
    private final AtomicBoolean replicationScheduled;
    private boolean running;
    private Future<?> future;
    private Runnable sendAppendEntriesRequest;

    public HeartbeatReplicationScheduler(long maxDelayBetweenMessagesInMilliseconds, ExecutorService executorService) {
        this.maxDelayBetweenMessagesInMilliseconds = maxDelayBetweenMessagesInMilliseconds;
        this.executorService = executorService;
        replicationScheduled = new AtomicBoolean(false);
        running = false;
    }

    @Override
    public void setSendAppendEntriesRequest(Runnable sendAppendEntriesRequest) {
        this.sendAppendEntriesRequest = sendAppendEntriesRequest;
    }

    @Override
    public void start() {
        running = true;
        future = executorService.submit(this::run);
    }

    @Override
    public void stop() {
        running = false;
        future.cancel(true);
    }

    private Void run() throws InterruptedException {
        try {
            while (running) {
                if (replicationScheduled.getAndSet(false)) {
                    sendAppendEntriesRequest.run();
                } else {
                    synchronized (replicationScheduled) {
                        replicationScheduled.wait(maxDelayBetweenMessagesInMilliseconds);
                    }
                    replicationScheduled.set(true);
                }
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Replication thread failed!", ex);
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
