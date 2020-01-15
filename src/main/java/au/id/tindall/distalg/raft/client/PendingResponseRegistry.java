package au.id.tindall.distalg.raft.client;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.statemachine.ClientSessionCreatedHandler;
import au.id.tindall.distalg.raft.statemachine.ClientSessionStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class PendingResponseRegistry {

    private final Map<Integer, PendingResponse<?>> pendingResponses;
    private final ClientSessionCreatedHandler clientSessionCreatedHandler;
    private final ClientSessionStore clientSessionStore;

    public PendingResponseRegistry(ClientSessionStore clientSessionStore) {
        this.pendingResponses = new HashMap<>();
        this.clientSessionCreatedHandler = this::handleSessionCreated;
        this.clientSessionStore = clientSessionStore;
        this.startListeningForClientRegistrations(clientSessionStore);
    }

    public <R extends ClientResponseMessage> CompletableFuture<R> registerOutstandingResponse(int logIndex, PendingResponse<R> pendingResponse) {
        pendingResponses.put(logIndex, pendingResponse);
        return pendingResponse.getResponseFuture();
    }

    private void startListeningForClientRegistrations(ClientSessionStore clientSessionStore) {
        clientSessionStore.addClientSessionCreatedHandler(this.clientSessionCreatedHandler);
    }

    private void stopListeningForClientRegistrations(ClientSessionStore clientSessionStore) {
        clientSessionStore.removeClientSessionCreatedHandler(this.clientSessionCreatedHandler);
    }

    private void failOutstandingResponses() {
        pendingResponses.values().forEach(PendingResponse::fail);
        pendingResponses.clear();
    }

    private void handleSessionCreated(int index, int clientId) {
        removePendingResponse(index).ifPresent(
                pendingResponse -> pendingResponse.getResponseFuture().complete(new RegisterClientResponse<>(RegisterClientStatus.OK, clientId, null)));
    }

    @SuppressWarnings("unchecked")
    private <T extends ClientResponseMessage> Optional<PendingResponse<T>> removePendingResponse(int index) {
        return Optional.ofNullable((PendingResponse<T>) pendingResponses.remove(index));
    }

    public void dispose() {
        this.stopListeningForClientRegistrations(clientSessionStore);
        this.failOutstandingResponses();
    }
}
