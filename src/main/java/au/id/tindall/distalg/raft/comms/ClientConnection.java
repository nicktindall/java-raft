package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientConnection<I extends Serializable> implements Closeable {

    private final Queue<CRHolder<?>> clientRequests;
    private final Object closingMutex = new Object();
    private boolean closed = false;

    public ClientConnection() {
        clientRequests = new ConcurrentLinkedQueue<>();
    }

    public <R extends ClientResponseMessage> CompletableFuture<R> send(ClientRequestMessage<R> clientRequestMessage) {
        synchronized (closingMutex) {
            if (closed) {
                throw new ConnectionClosedException();
            }
            CRHolder<R> holder = new CRHolder<>(clientRequestMessage);
            clientRequests.offer(holder);
            return holder.response;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @SuppressWarnings("unchecked")
    public <C extends ClientResponseMessage> boolean processAMessage(MessageProcessor<I> messageProcessor) {
        final CRHolder<C> crHolder = (CRHolder<C>) clientRequests.poll();
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
                closed = true;
            }
        }
    }

    private static class CRHolder<R extends ClientResponseMessage> {
        private final ClientRequestMessage<R> request;
        private final CompletableFuture<R> response;

        private CRHolder(ClientRequestMessage<R> clusterMembershipRequest) {
            this.request = clusterMembershipRequest;
            this.response = new CompletableFuture<>();
        }
    }
}