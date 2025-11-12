package au.id.tindall.distalg.raft.comms.netty;

import au.id.tindall.distalg.raft.clusterclient.ClusterClient;
import au.id.tindall.distalg.raft.comms.ConnectionClosedException;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.Message;
import au.id.tindall.distalg.raft.comms.netty.simplemesh.Node;
import au.id.tindall.distalg.raft.processors.SleepStrategies;
import au.id.tindall.distalg.raft.processors.SleepStrategy;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.util.Closeables;
import au.id.tindall.distalg.raft.util.ExceptionUtil;
import au.id.tindall.distalg.raft.util.ExecutorUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleMeshClusterClient implements ClusterClient<Integer>, Closeable {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Node localNode;
    private final AtomicLong messageCounter = new AtomicLong();
    private final Map<Long, CompletableFuture<?>> responseFutures = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final SleepStrategy sleepStrategy = SleepStrategies.threadSleep();

    public SimpleMeshClusterClient(Node localNode) {
        this.localNode = localNode;
        executor.submit(this::pollForMessages);
    }

    @Override
    public Set<Integer> getClusterNodeIds() {
        return localNode.getServerIds();
    }

    @Override
    public <R extends ClientResponseMessage<Integer>> CompletableFuture<R> send(Integer destination, ClientRequestMessage<Integer, R> clientRequestMessage) throws ConnectionClosedException {
        final long messageId = messageCounter.getAndIncrement();
        final CompletableFuture<R> responseFuture = new CompletableFuture<>();
        responseFutures.put(messageId, responseFuture);
        localNode.sendMessage(destination, new MessageEnvelope(messageId, clientRequestMessage));
        return responseFuture;
    }

    private void pollForMessages() {
        try {
            while (!closed.get()) {
                Optional<Message> poll = localNode.poll();
                poll.ifPresent(message -> {
                    MessageEnvelope envelope = (MessageEnvelope) message.message();
                    CompletableFuture completableFuture = responseFutures.get(envelope.correlationId());
                    if (completableFuture != null) {
                        completableFuture.complete(envelope.message());
                    } else {
                        LOGGER.warn("No matching response future: correlationId={}, message={}", envelope.correlationId(), envelope.message());
                    }
                });
                sleepStrategy.sleep();
            }
        } catch (Throwable t) {
            LOGGER.error("pollForMessages failed", t);
            ExceptionUtil.rethrowErrors(t);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            ExecutorUtil.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
            Closeables.closeQuietly(localNode);
        }
    }
}
