package au.id.tindall.distalg.raft.client;

import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

public interface PendingResponse<R extends ClientResponseMessage> {

    void completeSuccessfully(LogEntry entry);

    void fail();

    CompletableFuture<R> getResponseFuture();
}
