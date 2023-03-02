package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public interface ServerState<ID extends Serializable> {

    CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<ID> message);

    Result<ID> handle(RpcMessage<ID> message);

    ServerStateType getServerStateType();

    Log getLog();

    void enterState();

    void leaveState();

    CompletableFuture<? extends ClusterMembershipResponse> handle(ClusterMembershipRequest message);

    void requestVotes();
}
