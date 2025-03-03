package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public interface ServerState<I extends Serializable> {

    <R extends ClientResponseMessage> CompletableFuture<R> handle(ClientRequestMessage<I, R> message);

    Result<I> handle(RpcMessage<I> message);

    ServerStateType getServerStateType();

    Log getLog();

    void enterState();

    void leaveState();

    <R extends ServerAdminResponse> CompletableFuture<R> handle(ServerAdminRequest<R> message);

    void requestVotes(boolean earlyElection);
}
