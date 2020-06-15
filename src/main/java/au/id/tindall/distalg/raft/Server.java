package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.exceptions.AlreadyRunningException;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.server.InitiateElectionMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.Result;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Server<ID extends Serializable> {

    private final ID id;
    private final StateMachine stateMachine;
    private ServerState<ID> state;
    private ServerStateFactory<ID> serverStateFactory;

    public Server(ID id, ServerStateFactory<ID> serverStateFactory, StateMachine stateMachine) {
        this.id = id;
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

    public synchronized void handle(RpcMessage<ID> message) {
        assertThatNodeIsRunning();
        Result<ID> result;
        do {
            result = state.handle(message);
            updateState(result.getNextState());
        } while (!result.isFinished());
    }

    public synchronized void sendHeartbeatMessage() {
        state.sendHeartbeatMessage();
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
        handle(new InitiateElectionMessage<>(state.getCurrentTerm().next(), id));
    }

    public ID getId() {
        return id;
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
