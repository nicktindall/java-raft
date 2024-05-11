package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ServerAdminResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public interface MessageProcessor<I extends Serializable> {

    <R extends ClientResponseMessage> CompletableFuture<R> handle(ClientRequestMessage<I, R> clientRequestMessage);

    <R extends ServerAdminResponse> CompletableFuture<R> handle(ServerAdminRequest<R> serverAdminRequest);

    void handle(RpcMessage<I> message);
}
