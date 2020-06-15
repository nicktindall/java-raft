package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.Server;
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
    private Server<ID> server;

    public HeartbeatScheduler(long delayBetweenHeartbeatsInMilliseconds, ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.delayBetweenHeartbeatsInMilliseconds = delayBetweenHeartbeatsInMilliseconds;
    }

    public void setServer(Server<ID> server) {
        this.server = server;
    }

    public void scheduleHeartbeats() {
        LOGGER.debug("Scheduling heartbeats for leader");
        if (currentHeartbeatFuture != null) {
            throw new IllegalStateException("Heartbeats already scheduled!");
        }
        this.currentHeartbeatFuture = this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            LOGGER.debug("Sending heartbeat");
            server.sendHeartbeatMessage();
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
