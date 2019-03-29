package au.id.tindall.distalg.raft.client;

import static au.id.tindall.distalg.raft.rpc.RegisterClientStatus.NOT_LEADER;
import static au.id.tindall.distalg.raft.rpc.RegisterClientStatus.OK;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.RegisterClientResponse;

public class ClientRegistry<ID extends Serializable> {

    private final Map<Integer, CompletableFuture<RegisterClientResponse<ID>>> clientRegistrationFutures;
    private final EntryCommittedEventHandler entryCommittedEventHandler;

    public ClientRegistry() {
        this.clientRegistrationFutures = new HashMap<>();
        this.entryCommittedEventHandler = this::respondToClientRegistrationCommits;
    }

    public CompletableFuture<RegisterClientResponse<ID>> createResponseFuture(int clientId) {
        CompletableFuture<RegisterClientResponse<ID>> future = new CompletableFuture<>();
        clientRegistrationFutures.put(clientId, future);
        return future;
    }

    public void startListeningForCommitEvents(Log log) {
        log.addEntryCommittedEventHandler(this.entryCommittedEventHandler);
    }

    public void stopListeningForCommitEvents(Log log) {
        log.removeEntryCommittedEventHandler(this.entryCommittedEventHandler);
    }

    public void failAnyOutstandingRegistrations() {
        clientRegistrationFutures.values().forEach(
                future -> future.complete(new RegisterClientResponse<>(NOT_LEADER, null, null))
        );
        clientRegistrationFutures.clear();
    }

    void respondToClientRegistrationCommits(int index, LogEntry logEntry) {
        if (logEntry instanceof ClientRegistrationEntry) {
            ClientRegistrationEntry entry = (ClientRegistrationEntry) logEntry;
            clientRegistrationFutures.remove(entry.getClientId()).complete(new RegisterClientResponse<>(OK, entry.getClientId(), null));
        }
    }
}
