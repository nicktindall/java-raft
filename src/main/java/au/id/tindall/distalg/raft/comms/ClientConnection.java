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

    private final Queue<SARHolder> clusterMembershipRequests;
    private final Queue<CRHolder<I>> clientRequests;
    private final Object closingMutex = new Object();
    private boolean closed = false;

    public ClientConnection() {
        clientRequests = new ConcurrentLinkedQueue<>();
        clusterMembershipRequests = new ConcurrentLinkedQueue<>();
    }

    public CompletableFuture<ClientResponseMessage> send(ClientRequestMessage<I> clientRequestMessage) {
        synchronized (closingMutex) {
            if (closed) {
                throw new ConnectionClosedException();
            }
            CRHolder<I> holder = new CRHolder<>(clientRequestMessage);
            clientRequests.offer(holder);
            return holder.response;
        }
    }

    public CompletableFuture<ServerAdminResponse> send(ServerAdminRequest serverAdminRequest) {
        synchronized (closingMutex) {
            if (closed) {
                throw new ConnectionClosedException();
            }
            final SARHolder e = new SARHolder(serverAdminRequest);
            clusterMembershipRequests.offer(e);
            return e.response;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean processAMessage(MessageProcessor<I> messageProcessor) {
        final SARHolder sarHolder = clusterMembershipRequests.poll();
        if (sarHolder != null) {
            messageProcessor.handle(sarHolder.serverAdminRequest)
                    .thenAccept(sarHolder.response::complete);
            return true;
        }

        final CRHolder<I> crHolder = clientRequests.poll();
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

    private static class CRHolder<I extends Serializable> {
        private final ClientRequestMessage<I> request;
        private final CompletableFuture<ClientResponseMessage> response;

        private CRHolder(ClientRequestMessage<I> clusterMembershipRequest) {
            this.request = clusterMembershipRequest;
            this.response = new CompletableFuture<>();
        }
    }

    private static class SARHolder {

        private final ServerAdminRequest serverAdminRequest;
        private final CompletableFuture<ServerAdminResponse> response;

        private SARHolder(ServerAdminRequest serverAdminRequest) {
            this.serverAdminRequest = serverAdminRequest;
            this.response = new CompletableFuture<>();
        }
    }
}