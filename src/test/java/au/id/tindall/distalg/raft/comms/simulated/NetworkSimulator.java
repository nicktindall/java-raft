package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.processors.SleepStrategy;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

class NetworkSimulator<I> implements Runnable, Closeable {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<I, QueueingCluster<I>> clusters = new ConcurrentHashMap<>();
    private final Router<I> router;
    private final SleepStrategy sleepStrategy;
    private volatile boolean running = true;

    public NetworkSimulator(Router<I> router, SleepStrategy sleepStrategy) {
        this.router = router;
        this.sleepStrategy = sleepStrategy;
    }

    public Cluster<I> createCluster(I id, Consumer<RpcMessage<I>> messageSendListener) {
        return clusters.computeIfAbsent(id, i -> {
            QueueingCluster<I> qc = new QueueingCluster<>(i);
            if (messageSendListener != null) {
                qc.setMessageSendListener(messageSendListener);
            }
            return qc;
        });
    }

    public boolean flush() {
        return router.route(clusters);
    }

    @Override
    public void run() {
        try {
            while (running) {
                flush();
                sleepStrategy.sleep();
            }
            LOGGER.info("Network simulator stopped");
        } catch (Throwable e) {
            LOGGER.error("Network simulator failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
    }
}
