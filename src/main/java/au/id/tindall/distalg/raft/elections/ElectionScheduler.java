package au.id.tindall.distalg.raft.elections;

import au.id.tindall.distalg.raft.Server;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static au.id.tindall.distalg.raft.threading.NamedThreadFactory.forSingleThread;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class ElectionScheduler<ID extends Serializable> implements AutoCloseable {

    private static final Logger LOGGER = getLogger();

    private enum LifeCycle {
        New,
        Started,
        Stopped,
        Closed
    }

    private Server<?> server;
    private final ID serverId;
    private final ElectionTimeoutGenerator electionTimeoutGenerator;
    private final ExecutorService executorService;
    private final AtomicLong nextTimeoutTime;
    private final Supplier<Instant> timeProvider;
    private final AtomicReference<LifeCycle> lifeCycle;

    public ElectionScheduler(ID serverId, ElectionTimeoutGenerator electionTimeoutGenerator, Supplier<Instant> timeProvider) {
        this.serverId = serverId;
        this.electionTimeoutGenerator = electionTimeoutGenerator;
        this.timeProvider = timeProvider;
        this.executorService = newSingleThreadExecutor(forSingleThread("election-scheduler-" + serverId));
        nextTimeoutTime = new AtomicLong();
        lifeCycle = new AtomicReference<>(LifeCycle.New);
    }

    public void setServer(Server<?> server) {
        this.server = server;
    }

    public void startTimeouts() {
        if (lifeCycle.compareAndSet(LifeCycle.New, LifeCycle.Started)) {
            LOGGER.debug("Starting election timeouts for server {}", server.getId());
            executorService.submit(this::run);
        } else if (lifeCycle.compareAndSet(LifeCycle.Stopped, LifeCycle.Started)) {
            LOGGER.debug("Starting election timeouts for server {}", server.getId());
            scheduleTimeoutInFuture();
            wakeUpScheduler();
        } else {
            throw new IllegalStateException("Can't start timeouts, currently in state: " + lifeCycle.get());
        }
    }

    public void stopTimeouts() {
        if (lifeCycle.compareAndSet(LifeCycle.Started, LifeCycle.Stopped)) {
            LOGGER.debug("Stopping election timeouts for server {}", server.getId());
        } else {
            throw new IllegalStateException("Timeouts are not started");
        }
    }

    public void resetTimeout() {
        if (lifeCycle.get() != LifeCycle.Started) {
            throw new IllegalStateException("Timeouts are not started (lifeCycle=" + lifeCycle.get() + ")");
        }
        scheduleTimeoutInFuture();
    }

    private void run() {
        try (CloseableThreadContext.Instance ctc = CloseableThreadContext.put("serverId", serverId.toString())) {
            scheduleTimeoutInFuture();
            while (lifeCycle.get() != LifeCycle.Closed) {
                synchronized (nextTimeoutTime) {
                    switch (lifeCycle.get()) {
                        case Started:
                            final long millisUntilNextTimeout = nextTimeoutTime.get() - timeProvider.get().toEpochMilli();
                            nextTimeoutTime.wait(Math.max(0, millisUntilNextTimeout));
                            break;
                        case Stopped:
                            nextTimeoutTime.wait();
                            break;
                        default:
                            break;
                    }
                }
                // this is horrid but necessary
                synchronized (server) {
                    if (shouldTimeout()) {
                        LOGGER.debug("Election timeout occurred: server {}", serverId);
                        server.electionTimeout();
                        scheduleTimeoutInFuture();
                    }
                }
            }
            LOGGER.debug("Election scheduler closed, terminating");
        } catch (InterruptedException e) {
            LOGGER.error("Election scheduler thread was interrupted!");
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOGGER.warn("Election scheduler thread died", e);
        }
    }

    private boolean shouldTimeout() {
        return lifeCycle.get() == LifeCycle.Started
                && nextTimeoutTime.get() <= timeProvider.get().toEpochMilli();
    }

    private void scheduleTimeoutInFuture() {
        nextTimeoutTime.set(timeProvider.get().toEpochMilli() + electionTimeoutGenerator.next());
    }

    private void wakeUpScheduler() {
        synchronized (nextTimeoutTime) {
            nextTimeoutTime.notifyAll();
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (lifeCycle.compareAndSet(LifeCycle.Started, LifeCycle.Closed) || lifeCycle.compareAndSet(LifeCycle.Stopped, LifeCycle.Closed)) {
            LOGGER.debug("Closing scheduler {} from started/stopped", serverId);
            wakeUpScheduler();
            shutdownExecutor();
        } else if (lifeCycle.compareAndSet(LifeCycle.New, LifeCycle.Closed)) {
            LOGGER.debug("Closing scheduler {} from new", serverId);
            shutdownExecutor();
        }
    }

    private void shutdownExecutor() throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(3, SECONDS)) {
            LOGGER.error("Election scheduler thread didn't stop");
        }
    }
}
