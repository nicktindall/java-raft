package au.id.tindall.distalg.raft.client.responses;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionCreatedHandler;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.statemachine.CommandAppliedEventHandler;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class PendingResponseRegistry {

    private final Map<Integer, PendingResponse<?>> pendingResponses;
    private final CommandAppliedEventHandler commandAppliedEventHandler;
    private final ClientSessionCreatedHandler clientSessionCreatedHandler;
    private final ClientSessionStore clientSessionStore;
    private final CommandExecutor commandExecutor;

    public PendingResponseRegistry(ClientSessionStore clientSessionStore, CommandExecutor commandExecutor) {
        this.pendingResponses = new HashMap<>();
        this.clientSessionCreatedHandler = this::handleSessionCreated;
        this.clientSessionStore = clientSessionStore;
        this.commandAppliedEventHandler = this::handleCommandApplied;
        this.commandExecutor = commandExecutor;
        this.startListeningForClientRegistrations(clientSessionStore);
        this.startListeningForCommandApplications(commandExecutor);
    }

    public <R extends ClientResponseMessage> CompletableFuture<R> registerOutstandingResponse(int logIndex, PendingResponse<R> pendingResponse) {
        pendingResponses.put(logIndex, pendingResponse);
        return pendingResponse.getResponseFuture();
    }

    private void startListeningForCommandApplications(CommandExecutor commandExecutor) {
        commandExecutor.addCommandAppliedEventHandler(this.commandAppliedEventHandler);
    }

    private void stopListeningForCommandApplications(CommandExecutor commandExecutor) {
        commandExecutor.removeCommandAppliedEventHandler(this.commandAppliedEventHandler);
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

    private void handleCommandApplied(int logIndex, int clientId, int lastResponseReceived, int sequenceNumber, byte[] commandResult) {
        removePendingResponse(logIndex).ifPresent(
                pendingResponse -> pendingResponse.getResponseFuture().complete(new ClientRequestResponse<>(ClientRequestStatus.OK, commandResult, null)));
    }

    private void handleSessionCreated(int logIndex, int clientId) {
        removePendingResponse(logIndex).ifPresent(
                pendingResponse -> pendingResponse.getResponseFuture().complete(new RegisterClientResponse<>(RegisterClientStatus.OK, clientId, null)));
    }

    @SuppressWarnings("unchecked")
    private <T extends ClientResponseMessage> Optional<PendingResponse<T>> removePendingResponse(int index) {
        return Optional.ofNullable((PendingResponse<T>) pendingResponses.remove(index));
    }

    public void dispose() {
        this.stopListeningForClientRegistrations(clientSessionStore);
        this.stopListeningForCommandApplications(commandExecutor);
        this.failOutstandingResponses();
    }
}
