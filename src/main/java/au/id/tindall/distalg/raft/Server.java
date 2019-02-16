package au.id.tindall.distalg.raft;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.Candidate;
import au.id.tindall.distalg.raft.serverstates.Follower;
import au.id.tindall.distalg.raft.serverstates.Leader;
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

    public void handle(RpcMessage<ID> message) {
        revertToFollowerStateIfTermHasIncreased(message.getTerm());

        Result<ID> result;
        do {
            result = state.handle(message);
            state = result.getNextState();
        } while (!result.isFinished());
    }

    private void revertToFollowerStateIfTermHasIncreased(Term rpcTerm) {
        if (rpcTerm.isGreaterThan(state.getCurrentTerm())) {
            state = new Follower<>(state.getId(), rpcTerm,null, state.getLog(), state.getCluster());
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
            ((Candidate)state).requestVotes();
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

    public Set<ID> getReceivedVotes() {
        if (state instanceof Candidate) {
            return ((Candidate<ID>) state).getReceivedVotes();
        }
        return emptySet();
    }

    public int getCommitIndex() {
        return state.getCommitIndex();
    }

    public Map<ID, Integer> getNextIndices() {
        if (state instanceof Leader) {
            return ((Leader<ID>) state).getNextIndices();
        }
        return emptyMap();
    }

    public Map<ID, Integer> getMatchIndices() {
        if (state instanceof Leader) {
            return ((Leader<ID>) state).getMatchIndices();
        }
        return emptyMap();
    }
}
