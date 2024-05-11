package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminResponse;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientConnection<I extends Serializable> implements Closeable {

    private final Queue<SARHolder<?>> clusterMembershipRequests;
    private final Queue<CRHolder<I, ?>> clientRequests;
    private final Object closingMutex = new Object();
    private boolean closed = false;

    public ClientConnection() {
        clientRequests = new ConcurrentLinkedQueue<>();
        clusterMembershipRequests = new ConcurrentLinkedQueue<>();
    }

    public <R extends ClientResponseMessage> CompletableFuture<R> send(ClientRequestMessage<I, R> clientRequestMessage) {
        synchronized (closingMutex) {
            if (closed) {
                throw new ConnectionClosedException();
            }
            CRHolder<I, R> holder = new CRHolder<>(clientRequestMessage);
            clientRequests.offer(holder);
            return holder.response;
        }
    }

    public <R extends ServerAdminResponse> CompletableFuture<R> send(ServerAdminRequest<R> serverAdminRequest) {
        synchronized (closingMutex) {
            if (closed) {
                throw new ConnectionClosedException();
            }
            final SARHolder<R> e = new SARHolder<>(serverAdminRequest);
            clusterMembershipRequests.offer(e);
            return e.response;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @SuppressWarnings("unchecked")
    public <S extends ServerAdminResponse, C extends ClientResponseMessage> boolean processAMessage(MessageProcessor<I> messageProcessor) {
        final SARHolder<S> sarHolder = (SARHolder<S>) clusterMembershipRequests.poll();
        if (sarHolder != null) {
            messageProcessor.handle(sarHolder.serverAdminRequest)
                    .thenAccept(sarHolder.response::complete);
            return true;
        }

        final CRHolder<I, C> crHolder = (CRHolder<I, C>) clientRequests.poll();
        if (crHolder != null) {
            messageProcessor.handle(crHolder.request)
                    .thenAccept(crHolder.response::complete);
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        synchronized (closingMutex) {
            if (!closed) {
                clientRequests.forEach(cr -> cr.response.completeExceptionally(new ConnectionClosedException()));
                clientRequests.clear();
                clusterMembershipRequests.forEach(sarHolder -> sarHolder.response.completeExceptionally(new ConnectionClosedException()));
                clusterMembershipRequests.clear();
                closed = true;
            }
        }
    }

    private static class CRHolder<I extends Serializable, R extends ClientResponseMessage> {
        private final ClientRequestMessage<I, R> request;
        private final CompletableFuture<R> response;

        private CRHolder(ClientRequestMessage<I, R> clusterMembershipRequest) {
            this.request = clusterMembershipRequest;
            this.response = new CompletableFuture<>();
        }
    }

    private static class SARHolder<R extends ServerAdminResponse> {

        private final ServerAdminRequest<R> serverAdminRequest;
        private final CompletableFuture<R> response;

        private SARHolder(ServerAdminRequest<R> serverAdminRequest) {
            this.serverAdminRequest = serverAdminRequest;
            this.response = new CompletableFuture<>();
        }
    }
}