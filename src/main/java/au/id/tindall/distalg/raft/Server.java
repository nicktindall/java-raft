package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.driver.ServerDriver;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface Server<I extends Serializable> extends Closeable {

    boolean poll();

    boolean timeoutNowIfDue();

    void start();

    void start(ServerDriver serverDriver);

    void stop();

    CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<I> clientRequestMessage);

    CompletableFuture<? extends ClusterMembershipResponse> handle(ClusterMembershipRequest clusterMembershipRequest);

    void handle(RpcMessage<I> message);

    void initialize();

    void transferLeadership();

    I getId();

    Optional<ServerStateType> getState();

    Log getLog();

    ClientSessionStore getClientSessionStore();

    StateMachine getStateMachine();

    @Override
    void close();
}
