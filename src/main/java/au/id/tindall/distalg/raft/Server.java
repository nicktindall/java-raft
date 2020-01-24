package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.Candidate;
import au.id.tindall.distalg.raft.serverstates.Result;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public class Server<ID extends Serializable> {

    private final ID id;
    private final StateMachine stateMachine;
    private ServerState<ID> state;
    private ServerStateFactory<ID> serverStateFactory;

    public Server(ID id, ServerState<ID> state, ServerStateFactory<ID> serverStateFactory, StateMachine stateMachine) {
        this.id = id;
        this.state = state;
        this.serverStateFactory = serverStateFactory;
        this.stateMachine = stateMachine;
    }

    public Server(ID id, ServerStateFactory<ID> serverStateFactory, StateMachine stateMachine) {
        this(id, new Term(0), null, serverStateFactory, stateMachine);
    }

    public Server(ID id, Term currentTerm, ID votedFor, ServerStateFactory<ID> serverStateFactory, StateMachine stateMachine) {
        this(id, serverStateFactory.createFollower(currentTerm, votedFor), serverStateFactory, stateMachine);
    }

    public CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<ID> clientRequestMessage) {
        return state.handle(clientRequestMessage);
    }

    public void handle(RpcMessage<ID> message) {
        Result<ID> result;
        do {
            result = state.handle(message);
            updateState(result.getNextState());
        } while (!result.isFinished());
    }

    private void updateState(ServerState<ID> nextState) {
        if (state != nextState) {
            state.dispose();
            state = nextState;
        }
    }

    public void electionTimeout() {
        updateState(serverStateFactory.createCandidate(state.getCurrentTerm().next()));
        Result<ID> recordVoteResult = ((Candidate<ID>) state).recordVoteAndClaimLeadershipIfEligible(id);
        updateState(recordVoteResult.getNextState());
        if (state instanceof Candidate) {
            ((Candidate<ID>) state).requestVotes();
        }
    }

    public ID getId() {
        return id;
    }

    public ServerStateType getState() {
        return state.getServerStateType();
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
