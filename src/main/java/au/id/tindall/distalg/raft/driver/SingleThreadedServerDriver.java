package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.Server;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMicros;
import static org.apache.logging.log4j.LogManager.getLogger;

public class SingleThreadedServerDriver implements ServerDriver, Closeable, Runnable {

    private static final Logger LOGGER = getLogger();
    private static final AtomicInteger UNCLOSED_COUNTER = new AtomicInteger(0);
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Future<?>> future;
    private Server<?> server;


    public SingleThreadedServerDriver() {
        this.executorService = Executors.newSingleThreadExecutor();
        this.future = new AtomicReference<>();
        UNCLOSED_COUNTER.incrementAndGet();
    }

    @Override
    public void start(Server<?> server) {
        if (running.compareAndSet(false, true)) {
            this.server = server;
            future.set(executorService.submit(this));
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName("server-" + server.getId() + "-driver");
        try (CloseableThreadContext.Instance ctc = CloseableThreadContext.put("serverId", server.getId().toString())) {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                boolean receivedAMessage = server.poll();
                boolean timedOut = server.timeoutNowIfDue();
                if (!(receivedAMessage || timedOut)) {
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

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
//            try {
//                future.get().get();
//            } catch (InterruptedException | ExecutionException e) {
//                throw new RuntimeException(e);
//            }
        }
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        final int remainingDrivers = UNCLOSED_COUNTER.decrementAndGet();
        LOGGER.debug("Closed SingleThreadedServerDriver, leaving {} created but not closed", remainingDrivers);
    }
}
