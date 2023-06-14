package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public interface ServerState<I extends Serializable> {

    CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<I> message);

    Result<I> handle(RpcMessage<I> message);

    ServerStateType getServerStateType();

    Log getLog();

    void enterState();

    void leaveState();

    CompletableFuture<? extends ClusterMembershipResponse> handle(ClusterMembershipRequest message);

    void requestVotes();
}
