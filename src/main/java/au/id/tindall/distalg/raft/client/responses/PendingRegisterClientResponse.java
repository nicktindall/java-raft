package au.id.tindall.distalg.raft.client.responses;


import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.NOT_LEADER;

public class PendingRegisterClientResponse<I extends Serializable> implements PendingResponse<RegisterClientResponse<I>> {

    private final CompletableFuture<RegisterClientResponse<I>> future;

    public PendingRegisterClientResponse() {
        this.future = new CompletableFuture<>();
    }

    @Override
    public void fail() {
        future.complete(new RegisterClientResponse<>(NOT_LEADER, null, null));
    }

    @Override
    public CompletableFuture<RegisterClientResponse<I>> getResponseFuture() {
        return future;
    }
}
