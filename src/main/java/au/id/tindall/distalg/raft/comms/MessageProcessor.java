package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.util.concurrent.CompletableFuture;

public interface MessageProcessor<I> {

    <R extends ClientResponseMessage> CompletableFuture<R> handle(ClientRequestMessage<R> clientRequestMessage);

    void handle(RpcMessage<I> message);
}
