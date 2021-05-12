package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.exceptions.AlreadyRunningException;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.serverstates.Result;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Server<ID extends Serializable> {

    private final PersistentState<ID> persistentState;
    private final ServerStateFactory<ID> serverStateFactory;
    private final StateMachine stateMachine;
    private ServerState<ID> state;

    public Server(PersistentState<ID> persistentState, ServerStateFactory<ID> serverStateFactory, StateMachine stateMachine) {
        this.persistentState = persistentState;
        this.serverStateFactory = serverStateFactory;
        this.stateMachine = stateMachine;
    }

    public synchronized void start() {
        if (state != null) {
            throw new AlreadyRunningException("Can't start, server is already started!");
        }
        updateState(serverStateFactory.createInitialState());
    }

    public synchronized void stop() {
        if (state == null) {
            throw new NotRunningException("Can't stop, server is not started");
        }
        updateState(null);
    }

    public synchronized CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<ID> clientRequestMessage) {
        assertThatNodeIsRunning();
        return state.handle(clientRequestMessage);
    }

    public synchronized CompletableFuture<? extends ClusterMembershipResponse> handle(ClusterMembershipRequest clusterMembershipRequest) {
        assertThatNodeIsRunning();
        return state.handle(clusterMembershipRequest);
    }

    public synchronized void handle(RpcMessage<ID> message) {
        assertThatNodeIsRunning();
        Result<ID> result;
        do {
            result = state.handle(message);
            updateState(result.getNextState());
        } while (!result.isFinished());
    }

    private void assertThatNodeIsRunning() {
        if (state == null) {
            throw new NotRunningException("Server is not running, call start() before attempting to interact with it");
        }
    }

    private void updateState(ServerState<ID> nextState) {
        if (state != nextState) {
            if (state != null) {
                state.leaveState();
            }
            state = nextState;
            if (nextState != null) {
                state.enterState();
            }
        }
    }

    public synchronized void electionTimeout() {
        handle(new TimeoutNowMessage<>(persistentState.getCurrentTerm(), persistentState.getId(), persistentState.getId()));
    }

    public synchronized void transferLeadership() {
        handle(new TransferLeadershipMessage<>(persistentState.getCurrentTerm(), persistentState.getId()));
    }

    public ID getId() {
        return persistentState.getId();
    }

    public Optional<ServerStateType> getState() {
        return Optional.ofNullable(state)
                .map(ServerState::getServerStateType);
    }

    public Log getLog() {
        return state.getLog();
    }

    public ClientSessionStore getClientSessionStore() {
        return serverStateFactory.getClientSessionStore();
    }

    public StateMachine getStateMachine() {
        return this.stateMachine;
    }
}
