package au.id.tindall.distalg.raft.elections;

import au.id.tindall.distalg.raft.Server;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class ElectionScheduler<ID extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private Server<ID> server;
    private final ElectionTimeoutGenerator electionTimeoutGenerator;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicReference<ScheduledFuture<?>> nextElectionTimeout;
    private volatile boolean timeoutsRunning = false;

    public ElectionScheduler(ElectionTimeoutGenerator electionTimeoutGenerator, ScheduledExecutorService scheduledExecutorService) {
        this.electionTimeoutGenerator = electionTimeoutGenerator;
        this.scheduledExecutorService = scheduledExecutorService;
        nextElectionTimeout = new AtomicReference<>();
    }

    public void setServer(Server<ID> server) {
        this.server = server;
    }

    public synchronized void startTimeouts() {
        LOGGER.debug("Starting election timeouts for server {}", server.getId());
        if (timeoutsRunning) {
            throw new IllegalStateException("Timeouts are already started");
        }
        timeoutsRunning = true;
        scheduleElectionTimeout();
    }

    public synchronized void resetTimeout() {
        if (!timeoutsRunning) {
            throw new IllegalStateException("Timeouts are not started");
        }
        cancelPendingTimeout();
        scheduleElectionTimeout();
    }

    public synchronized void stopTimeouts() {
        LOGGER.debug("Stopping election timeouts for server {}", server.getId());
        if (!timeoutsRunning) {
            throw new IllegalStateException("Timeouts are not started");
        }
        timeoutsRunning = false;
        cancelPendingTimeout();
    }

    private void cancelPendingTimeout() {
        Future<?> timeout = nextElectionTimeout.get();
        if (timeout != null) {
            LOGGER.trace("Cancelling election timeout... ({})", server.getId());
            boolean cancel = timeout.cancel(false);
            if (cancel) {
                LOGGER.trace("Successfully cancelled timeout ({})", server.getId());
            } else {
                LOGGER.trace("Couldn't cancel previous timeout ({})", server.getId());
            }
            nextElectionTimeout.set(null);
        }
    }

    private void scheduleElectionTimeout() {
        Long next = electionTimeoutGenerator.next();
        LOGGER.trace("Scheduling election timeout for {} milliseconds ({})", next, server.getId());
        nextElectionTimeout.set(scheduledExecutorService.schedule(
                this::performElectionTimeout,
                next,
                MILLISECONDS));
    }

    @SuppressWarnings("unused")
    private void performElectionTimeout() {
        try (CloseableThreadContext.Instance ctc = CloseableThreadContext.put("serverId", server.getId().toString())) {
            LOGGER.debug("Election timeout occurred: server {}", server.getId());
            nextElectionTimeout.set(null);
            server.electionTimeout();
            resetTimeout();
        }
    }
}
