package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
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
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void setDispatchFunction(Consumer<RpcMessage<Long>> dispatchFunction) {
        this.dispatchFunction = dispatchFunction;
    }

    @Override
    public void send(RpcMessage<Long> message) {
        long nextDelay = minimumMessageDelayMillis + (random.nextInt(maximumMessageDelayMillis - minimumMessageDelayMillis));
        scheduledExecutorService.schedule(createDispatch(message),
                nextDelay,
                TimeUnit.MILLISECONDS);
    }

    private Runnable createDispatch(RpcMessage<Long> message) {
        return () -> {
            try {
                this.dispatchFunction.accept(message);
            } catch (RuntimeException ex) {
                LOGGER.error("Message sending threw", ex);
            }
        };
    }
}
