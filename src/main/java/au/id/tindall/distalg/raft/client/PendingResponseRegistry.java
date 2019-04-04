package au.id.tindall.distalg.raft.client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

public class PendingResponseRegistry {

    private final Map<Integer, PendingResponse> pendingResponses;
    private final EntryCommittedEventHandler entryCommittedEventHandler;

    public PendingResponseRegistry() {
        this.pendingResponses = new HashMap<>();
        this.entryCommittedEventHandler = this::handleEntryCommitted;
    }

    public <R extends ClientResponseMessage> CompletableFuture<R> registerOutstandingResponse(int logIndex, PendingResponse<R> pendingResponse) {
        pendingResponses.put(logIndex, pendingResponse);
        return pendingResponse.getResponseFuture();
    }

    public void startListeningForCommitEvents(Log log) {
        log.addEntryCommittedEventHandler(this.entryCommittedEventHandler);
    }

    public void stopListeningForCommitEvents(Log log) {
        log.removeEntryCommittedEventHandler(this.entryCommittedEventHandler);
    }

    public void failOutstandingResponses() {
        pendingResponses.values().forEach(PendingResponse::fail);
        pendingResponses.clear();
    }

    void handleEntryCommitted(int index, LogEntry logEntry) {
        if (pendingResponses.containsKey(index)) {
            pendingResponses.get(index).completeSuccessfully(logEntry);
        }
    }
}
