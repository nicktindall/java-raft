package au.id.tindall.distalg.raft.clusterclient;

import au.id.tindall.distalg.raft.comms.ConnectionClosedException;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface ClusterClient<I> {

    Set<I> getClusterNodeIds();

    <R extends ClientResponseMessage<I>> CompletableFuture<R> send(I destination, ClientRequestMessage<I, R> clientRequestMessage) throws ConnectionClosedException;
}
