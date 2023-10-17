package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public interface MessageProcessor<I extends Serializable> {

    CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<I> clientRequestMessage);

    CompletableFuture<? extends ServerAdminResponse> handle(ServerAdminRequest serverAdminRequest);

    void handle(RpcMessage<I> message);
}
