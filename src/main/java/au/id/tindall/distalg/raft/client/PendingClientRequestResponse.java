package au.id.tindall.distalg.raft.client;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.NOT_LEADER;

public class PendingClientRequestResponse<ID extends Serializable> implements PendingResponse<ClientRequestResponse<ID>> {

    private final CompletableFuture<ClientRequestResponse<ID>> future;

    public PendingClientRequestResponse() {
        this.future = new CompletableFuture<>();
    }

    @Override
    public void fail() {
        future.complete(new ClientRequestResponse<>(NOT_LEADER, null, null));
    }

    @Override
    public CompletableFuture<ClientRequestResponse<ID>> getResponseFuture() {
        return future;
    }
}
