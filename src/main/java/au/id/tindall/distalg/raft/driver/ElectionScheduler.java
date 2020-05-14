package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.Server;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class ElectionScheduler<ID extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private Server<ID> server;
    private final ElectionTimeoutGenerator electionTimeoutGenerator;
    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> nextElectionTimeout;
    private boolean timeoutsRunning = false;

    public ElectionScheduler(ElectionTimeoutGenerator electionTimeoutGenerator, ScheduledExecutorService scheduledExecutorService) {
        this.electionTimeoutGenerator = electionTimeoutGenerator;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void setServer(Server<ID> server) {
        this.server = server;
    }

    public synchronized void startTimeouts() {
        LOGGER.debug("Starting election timeouts for server " + server.getId());
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
        LOGGER.debug("Stopping election timeouts for server " + server.getId());
        if (!timeoutsRunning) {
            throw new IllegalStateException("Timeouts are not started");
        }
        timeoutsRunning = false;
        cancelPendingTimeout();
    }

    private void cancelPendingTimeout() {
        if (nextElectionTimeout != null) {
            LOGGER.debug("Cancelling election timeout... (" + server.getId() + ")");
            boolean cancel = nextElectionTimeout.cancel(false);
            if (cancel) {
                LOGGER.debug("Successfully cancelled timeout (" + server.getId() + ")");
            } else {
                LOGGER.debug("Couldn't cancel previous timeout (" + server.getId() + ")");
            }
            nextElectionTimeout = null;
        }
    }

    private void scheduleElectionTimeout() {
        Long next = electionTimeoutGenerator.next();
        LOGGER.debug("Scheduling election timeout for " + next + " milliseconds (" + server.getId() + ")");
        nextElectionTimeout = scheduledExecutorService.schedule(
                this::performElectionTimeout,
                next,
                MILLISECONDS);
    }

    private synchronized void performElectionTimeout() {
        LOGGER.debug("Election timeout occurred: server " + server.getId());
        nextElectionTimeout = null;
        server.electionTimeout();
        resetTimeout();
    }
}
