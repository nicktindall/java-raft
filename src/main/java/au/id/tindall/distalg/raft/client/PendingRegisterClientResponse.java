package au.id.tindall.distalg.raft.client;


import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.NOT_LEADER;
import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.OK;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;

public class PendingRegisterClientResponse<ID extends Serializable> implements PendingResponse<RegisterClientResponse<ID>> {

    private CompletableFuture<RegisterClientResponse<ID>> future;

    public PendingRegisterClientResponse() {
        this.future = new CompletableFuture<>();
    }

    @Override
    public void completeSuccessfully(LogEntry entry) {
        future.complete(new RegisterClientResponse<>(OK, ((ClientRegistrationEntry) entry).getClientId(), null));
    }

    @Override
    public void fail() {
        future.complete(new RegisterClientResponse<>(NOT_LEADER, null, null));
    }

    @Override
    public CompletableFuture<RegisterClientResponse<ID>> getResponseFuture() {
        return future;
    }
}
