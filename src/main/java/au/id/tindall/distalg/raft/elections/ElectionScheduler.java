package au.id.tindall.distalg.raft.elections;

import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.logging.log4j.LogManager.getLogger;

public class ElectionScheduler {
    private static final Logger LOGGER = getLogger();

    private static final long UNSET_TIMEOUT = Long.MAX_VALUE;
    private final long minimumElectionTimeoutMilliseconds;
    private final ElectionTimeoutGenerator electionTimeoutGenerator;
    private final AtomicLong nextTimeoutTime;
    private final Supplier<Instant> timeProvider;
    private final AtomicReference<Instant> lastHeartbeatTime;

    public ElectionScheduler(long minimumElectionTimeoutMilliseconds, ElectionTimeoutGenerator electionTimeoutGenerator, Supplier<Instant> timeProvider) {
        this.minimumElectionTimeoutMilliseconds = minimumElectionTimeoutMilliseconds;
        this.electionTimeoutGenerator = electionTimeoutGenerator;
        this.timeProvider = timeProvider;
        nextTimeoutTime = new AtomicLong(UNSET_TIMEOUT);
        lastHeartbeatTime = new AtomicReference<>(Instant.EPOCH);
    }

    public void updateHeartbeat() {
        lastHeartbeatTime.set(timeProvider.get());
    }

    public void resetTimeout() {
        if (nextTimeoutTime.get() == UNSET_TIMEOUT) {
            throw new IllegalStateException("Timeouts are not started");
        }
        scheduleTimeoutInFuture();
    }

    public boolean isHeartbeatCurrent() {
        return Duration.between(lastHeartbeatTime.get(), timeProvider.get()).toMillis() < minimumElectionTimeoutMilliseconds;
    }

    public boolean shouldTimeout() {
        return nextTimeoutTime.get() <= timeProvider.get().toEpochMilli();
    }

    private void scheduleTimeoutInFuture() {
        nextTimeoutTime.set(timeProvider.get().toEpochMilli() + electionTimeoutGenerator.next());
    }

    public void startTimeouts() {
        if (nextTimeoutTime.get() != UNSET_TIMEOUT) {
            throw new IllegalStateException("Already started timeouts");
        }
        LOGGER.debug("Starting election timeouts");
        scheduleTimeoutInFuture();
    }

    public void stopTimeouts() {
        if (nextTimeoutTime.get() == UNSET_TIMEOUT) {
            throw new IllegalStateException("Timeouts are not started");
        }
        LOGGER.debug("Stopping election timeouts");
        nextTimeoutTime.set(UNSET_TIMEOUT);
    }
}
