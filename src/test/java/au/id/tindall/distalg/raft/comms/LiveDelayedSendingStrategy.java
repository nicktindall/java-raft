package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.threading.NamedThreadFactory;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.logging.log4j.LogManager.getLogger;

public class LiveDelayedSendingStrategy implements SendingStrategy {

    private static final Logger LOGGER = getLogger();

    private final int maximumMessageDelayMillis;
    private final int minimumMessageDelayMillis;
    private final Random random;
    private final ScheduledExecutorService scheduledExecutorService;
    private Consumer<RpcMessage<Long>> dispatchFunction;

    public LiveDelayedSendingStrategy(int minimumMessageDelayMillis, int maximumMessageDelayMillis) {
        if (minimumMessageDelayMillis >= maximumMessageDelayMillis) {
            throw new IllegalArgumentException("Minimum message delay must be less than maximum message delay");
        }
        this.minimumMessageDelayMillis = minimumMessageDelayMillis;
        this.maximumMessageDelayMillis = maximumMessageDelayMillis;
        this.random = new Random();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(10, new NamedThreadFactory("dispatch"));
    }

    @Override
    public void setDispatchFunction(Consumer<RpcMessage<Long>> dispatchFunction) {
        this.dispatchFunction = dispatchFunction;
    }

    @Override
    public void send(RpcMessage<Long> message) {
        long nextDelay = minimumMessageDelayMillis + (random.nextInt(maximumMessageDelayMillis - minimumMessageDelayMillis));
        scheduledExecutorService.schedule(() -> this.dispatchFunction.accept(message),
                nextDelay,
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        long startTime = System.currentTimeMillis();
        this.scheduledExecutorService.shutdownNow();
        try {
            if (!this.scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.error("Sending executor didn't stop");
            } else {
                LOGGER.debug("Took {} ms to shutdown", System.currentTimeMillis() - startTime);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
