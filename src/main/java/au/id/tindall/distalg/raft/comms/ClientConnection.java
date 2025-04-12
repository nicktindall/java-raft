package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientConnection<I> implements Closeable {

    private final Queue<CRHolder<?, ?>> clientRequests;
    private final Object closingMutex = new Object();
    private boolean closed = false;

    public ClientConnection() {
        clientRequests = new ConcurrentLinkedQueue<>();
    }

    public <R extends ClientResponseMessage<I>> CompletableFuture<R> send(ClientRequestMessage<I, R> clientRequestMessage) {
        synchronized (closingMutex) {
            if (closed) {
                throw new ConnectionClosedException();
            }
            CRHolder<I, R> holder = new CRHolder<>(clientRequestMessage);
            clientRequests.offer(holder);
            return holder.response;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @SuppressWarnings("unchecked")
    public <R extends ClientResponseMessage<I>> boolean processAMessage(MessageProcessor<I> messageProcessor) {
        final CRHolder<I, R> crHolder = (CRHolder<I, R>) clientRequests.poll();
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

    private static class CRHolder<I, R extends ClientResponseMessage<I>> {
        private final ClientRequestMessage<I, R> request;
        private final CompletableFuture<R> response;

        private CRHolder(ClientRequestMessage<I, R> clusterMembershipRequest) {
            this.request = clusterMembershipRequest;
            this.response = new CompletableFuture<>();
        }
    }
}