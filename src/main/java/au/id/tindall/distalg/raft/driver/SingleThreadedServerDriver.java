package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.Server;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMicros;
import static org.apache.logging.log4j.LogManager.getLogger;

public class SingleThreadedServerDriver implements ServerDriver, Closeable, Runnable {

    private static final int BUSY_WAIT_MILLISECONDS = 5;
    private static final int BUSY_WAIT_NANOSECONDS = BUSY_WAIT_MILLISECONDS * 1_000_000;

    enum LifeCycle {
        Initialised,
        Started,
        Stopped,
        Closed
    }

    private static final Logger LOGGER = getLogger();
    private static final AtomicInteger UNCLOSED_COUNTER = new AtomicInteger(0);
    private final ExecutorService executorService;
    private final AtomicReference<LifeCycle> lifeCycle = new AtomicReference<>(LifeCycle.Initialised);
    private final AtomicReference<Future<?>> future;
    private long lastEventTimeNanos = 0;
    private Server<?> server;


    public SingleThreadedServerDriver() {
        this.executorService = Executors.newSingleThreadExecutor();
        this.future = new AtomicReference<>();
        UNCLOSED_COUNTER.incrementAndGet();
    }

    @Override
    public void start(Server<?> server) {
        if (lifeCycle.compareAndSet(LifeCycle.Initialised, LifeCycle.Started)) {
            this.server = server;
            future.set(executorService.submit(this));
        } else {
            LOGGER.debug("Couldn't start, in state {}", lifeCycle.get());
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName("server-" + server.getId() + "-driver");
        try (CloseableThreadContext.Instance ctc = CloseableThreadContext.put("serverId", server.getId().toString())) {
            while (lifeCycle.get() == LifeCycle.Started && !Thread.currentThread().isInterrupted()) {
                boolean receivedAMessage = server.poll();
                boolean timedOut = server.timeoutNowIfDue();
                if (receivedAMessage || timedOut) {
                    lastEventTimeNanos = System.nanoTime();
                } else {
                    if (System.nanoTime() - lastEventTimeNanos < BUSY_WAIT_NANOSECONDS) {
                        Thread.onSpinWait();
                    } else {
                        long startTime = System.currentTimeMillis();
                        pauseMicros(300);
                        final long pauseTime = System.currentTimeMillis() - startTime;
                        if (pauseTime > 20) {
                            LOGGER.warn("Pause went for {}ms", pauseTime);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void stop() {
        if (lifeCycle.compareAndSet(LifeCycle.Initialised, LifeCycle.Stopped)
                || lifeCycle.compareAndSet(LifeCycle.Started, LifeCycle.Stopped)) {
//            try {
//                future.get().get();
//            } catch (InterruptedException | ExecutionException e) {
//                throw new RuntimeException(e);
//            }
        } else {
            LOGGER.debug("Couldn't stop, in state {}", lifeCycle.get());
        }
    }

    @Override
    public void close() throws IOException {
        stop();
        if (lifeCycle.compareAndSet(LifeCycle.Stopped, LifeCycle.Closed)) {
            executorService.shutdown();
            final int remainingDrivers = UNCLOSED_COUNTER.decrementAndGet();
            LOGGER.debug("Closed SingleThreadedServerDriver, leaving {} created but not closed", remainingDrivers);
        } else {
            LOGGER.debug("Couldn't close, in state {}", lifeCycle.get());
        }
    }
}
