package au.id.tindall.distalg.raft.clusterclient;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.threading.NamedThreadFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public abstract class AbstractClusterClient<I> implements Closeable {

    private static final long MAX_DELAY_MILLIS = 500;
    private static final int MAX_RETRIES = 20;
    private static final long INITIAL_DELAY_MILLIS = 10;
    private static final double BACKOFF_FACTOR = 2;

    private final ClusterClient<I> clusterClient;
    private final long initialDelayMillis;
    private final double backoffFactor;
    private final int maxRetries;
    private final long maxDelayMillis;
    private final ConcurrentHashMap<I, ConnectMetadata> servers = new ConcurrentHashMap<>();
    private final AtomicReference<ConnectMetadata> leader = new AtomicReference<>();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(5, NamedThreadFactory.forThreadGroup("client-threads"));
    private final AtomicInteger outstandingRequests = new AtomicInteger(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected AbstractClusterClient(ClusterClient<I> clusterClient) {
        this(clusterClient, INITIAL_DELAY_MILLIS, BACKOFF_FACTOR, MAX_RETRIES, MAX_DELAY_MILLIS);
    }

    protected AbstractClusterClient(ClusterClient<I> connection, long initialDelayMillis, double backoffFactor, int maxRetries, long maxDelayMillis) {
        this.clusterClient = connection;
        this.initialDelayMillis = initialDelayMillis;
        this.backoffFactor = backoffFactor;
        this.maxRetries = maxRetries;
        this.maxDelayMillis = maxDelayMillis;
    }

    protected <R extends ClientResponseMessage<I>> CompletableFuture<R> sendClientRequest(ClientRequestMessage<I, R> request, long requestTimeoutMs) {
        incrementOutstandingRequests();
        return attemptSend(node -> clusterClient.send(node, request), requestTimeoutMs)
                .whenComplete((result, ex) -> decrementOutstandingRequests());

    }

    private <R extends ClientResponseMessage<I>> CompletableFuture<R> attemptSend(Function<I, CompletableFuture<R>> messageSender, long requestTimeoutMs) {
        return attemptSend(messageSender, requestTimeoutMs, maxRetries, initialDelayMillis, new ArrayList<>(maxRetries));
    }

    private <R extends ClientResponseMessage<I>> CompletableFuture<R> attemptSend(Function<I, CompletableFuture<R>> messageSender, long requestTimeoutMs, int retriesRemaining, long delay, List<Exception> suppressed) {
        ConnectMetadata leaderConnection = getLeaderConnection();
        leaderConnection.lastAttemptedSendTimestamp = System.currentTimeMillis();
        I leaderId = leaderConnection.nodeId;
        return messageSender.apply(leaderId).orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .thenComposeAsync(res -> {
                    if (res.isFromLeader()) {
                        leader.set(leaderConnection);
                        return CompletableFuture.completedFuture(res);
                    }
                    if (!Objects.equals(leaderId, res.getLeaderHint())) {
                        leader.compareAndSet(leaderConnection, getConnectMetadata(res.getLeaderHint()));
                    } else {
                        leader.compareAndSet(leaderConnection, null);
                    }
                    throw new NotLeaderException();
                }, executor)
                .exceptionallyComposeAsync(t -> {
                    final Throwable cause = unwrapCause(t);
                    if (cause instanceof Exception exception && retriesRemaining > 0 && !closed.get()) {
                        suppressed.add(exception);
                        return attemptSend(messageSender, requestTimeoutMs, retriesRemaining - 1, calculateNextDelay(delay), suppressed);
                    } else {
                        SendFailedException sendFailedException = new SendFailedException();
                        suppressed.forEach(sendFailedException::addSuppressed);
                        return CompletableFuture.failedFuture(sendFailedException);
                    }
                }, retriesRemaining > 0 ? CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS, executor) : executor);
    }

    private Throwable unwrapCause(Throwable e) {
        if (e instanceof CompletionException compEx) {
            return compEx.getCause();
        }
        return e;
    }

    public static class SendFailedException extends RuntimeException {
    }

    public static class ClientClosedException extends SendFailedException {
    }

    public static class NotLeaderException extends SendFailedException {
    }

    private static class NoConnectionsException extends SendFailedException {
    }

    private long calculateNextDelay(long currentDelay) {
        return (long) Math.min((currentDelay * backoffFactor), maxDelayMillis);
    }

    private ConnectMetadata getConnectMetadata(I leaderId) {
        if (leaderId == null) {
            return null;
        }
        if (clusterClient.getClusterNodeIds().contains(leaderId)) {
            return servers.computeIfAbsent(leaderId, ConnectMetadata::new);
        }
        return null;
    }

    private ConnectMetadata getLeaderConnection() {
        ConnectMetadata nodeToSendTo = leader.get();
        if (nodeToSendTo == null) {
            clusterClient.getClusterNodeIds().forEach(node -> servers.computeIfAbsent(node, ConnectMetadata::new));
            nodeToSendTo = servers.values().stream().min(Comparator.comparing(cmd -> cmd.lastAttemptedSendTimestamp)).orElseThrow(NoConnectionsException::new);
            leader.compareAndSet(null, nodeToSendTo);
        }
        return nodeToSendTo;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            decrementOutstandingRequests();
        }
    }

    private void incrementOutstandingRequests() {
        while (true) {
            int currentValue = outstandingRequests.get();
            if (currentValue == 0) {
                throw new ClientClosedException();
            }
            if (outstandingRequests.compareAndSet(currentValue, currentValue + 1)) {
                break;
            }
        }
    }

    private void decrementOutstandingRequests() {
        int newValue = outstandingRequests.decrementAndGet();
        if (newValue == 0) {
            executor.shutdown();
        }
    }

    private class ConnectMetadata {
        private final I nodeId;
        private volatile long lastAttemptedSendTimestamp;

        private ConnectMetadata(I nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public String toString() {
            return "ConnectMetadata{" +
                    "nodeId=" + nodeId +
                    '}';
        }
    }
}
