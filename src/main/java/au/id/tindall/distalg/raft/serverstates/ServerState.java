package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.util.concurrent.CompletableFuture;

public interface ServerState<I> {

    <R extends ClientResponseMessage<I>> CompletableFuture<R> handle(ClientRequestMessage<I, R> message);

    Result<I> handle(RpcMessage<I> message);

    ServerStateType getServerStateType();

    Log getLog();

    void enterState();

    void leaveState();

    void requestVotes(boolean earlyElection);
}
