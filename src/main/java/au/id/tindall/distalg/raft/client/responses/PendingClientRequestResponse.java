package au.id.tindall.distalg.raft.client.responses;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;

import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.NOT_LEADER;

public class PendingClientRequestResponse<I> implements PendingResponse<ClientRequestResponse<I>> {

    private final CompletableFuture<ClientRequestResponse<I>> future;

    public PendingClientRequestResponse() {
        this.future = new CompletableFuture<>();
    }

    @Override
    public void fail() {
        future.complete(new ClientRequestResponse<>(NOT_LEADER, null, null));
    }

    @Override
    public CompletableFuture<ClientRequestResponse<I>> getResponseFuture() {
        return future;
    }
}
