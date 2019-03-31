package au.id.tindall.distalg.raft;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.Candidate;
import au.id.tindall.distalg.raft.serverstates.Follower;
import au.id.tindall.distalg.raft.serverstates.Result;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;

public class Server<ID extends Serializable> {

    private ServerState<ID> state;

    public Server(ServerState<ID> state) {
        this.state = state;
    }

    public Server(ID id, Cluster<ID> cluster) {
        this(id, new Term(0), null, new Log(), cluster);
    }

    public Server(ID id, Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster) {
        state = new Follower<>(id, currentTerm, votedFor, log, cluster);
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
        Candidate<ID> nextState = new Candidate<>(
                state.getId(),
                state.getCurrentTerm().next(),
                state.getLog(),
                state.getCluster());
        state = nextState.recordVoteAndClaimLeadershipIfEligible(state.getId()).getNextState();
        if (state instanceof Candidate) {
            ((Candidate) state).requestVotes();
        }
    }

    public ID getId() {
        return state.getId();
    }

    public Term getCurrentTerm() {
        return state.getCurrentTerm();
    }

    public ServerStateType getState() {
        return state.getServerStateType();
    }

    public Optional<ID> getVotedFor() {
        return state.getVotedFor();
    }

    public Log getLog() {
        return state.getLog();
    }
}
