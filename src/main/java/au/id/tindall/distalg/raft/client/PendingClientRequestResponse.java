package au.id.tindall.distalg.raft.client;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.NOT_LEADER;
import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.OK;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

public class PendingClientRequestResponse<ID extends Serializable> implements PendingResponse<ClientRequestResponse<ID>> {

    private final StateMachine stateMachine;
    private final CompletableFuture<ClientRequestResponse<ID>> future;

    public PendingClientRequestResponse(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
        this.future = new CompletableFuture<>();
    }

    @Override
    public void completeSuccessfully(LogEntry entry) {
        future.complete(new ClientRequestResponse<>(OK, stateMachine.apply(((StateMachineCommandEntry) entry).getCommand()), null));
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
