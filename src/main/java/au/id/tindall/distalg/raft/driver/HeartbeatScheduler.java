package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.serverstates.Leader;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class HeartbeatScheduler<ID extends Serializable> {

    private static final Logger LOGGER = getLogger();

    private final ScheduledExecutorService scheduledExecutorService;
    private final long delayBetweenHeartbeatsInMilliseconds;
    private ScheduledFuture<?> currentHeartbeatFuture;

    public HeartbeatScheduler(long delayBetweenHeartbeatsInMilliseconds, ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.delayBetweenHeartbeatsInMilliseconds = delayBetweenHeartbeatsInMilliseconds;
    }

    public void scheduleHeartbeats(Leader<ID> leader) {
        LOGGER.debug("Scheduling heartbeats for leader");
        if (currentHeartbeatFuture != null) {
            throw new IllegalStateException("Heartbeats already scheduled!");
        }
        this.currentHeartbeatFuture = this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            LOGGER.debug("Sending heartbeat");
            leader.sendHeartbeatMessage();
        }, 0, delayBetweenHeartbeatsInMilliseconds, MILLISECONDS);
    }

    public void cancelHeartbeats() {
        LOGGER.debug("Cancelling heartbeats for leader");
        if (currentHeartbeatFuture == null) {
            throw new IllegalStateException("Heartbeats not scheduled!");
        }
        this.currentHeartbeatFuture.cancel(false);
        this.currentHeartbeatFuture = null;
    }
}
