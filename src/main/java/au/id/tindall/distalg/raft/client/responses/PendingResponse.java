package au.id.tindall.distalg.raft.client.responses;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

import java.util.concurrent.CompletableFuture;

public interface PendingResponse<R extends ClientResponseMessage> {

    void fail();

    CompletableFuture<R> getResponseFuture();
}
