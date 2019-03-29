package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static java.util.Collections.unmodifiableSet;
import static java.util.Optional.empty;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import au.id.tindall.distalg.raft.client.ClientRegistryFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;

public class Candidate<ID extends Serializable> extends ServerState<ID> {

    private final Set<ID> receivedVotes;

    public Candidate(ID id, Term currentTerm, Log log, Cluster<ID> cluster) {
        super(id, currentTerm, id, log, cluster);
        receivedVotes = new HashSet<>();
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (messageIsStale(appendEntriesRequest)) {
            getCluster().send(new AppendEntriesResponse<>(getCurrentTerm(), getId(), appendEntriesRequest.getLeaderId(), false, empty()));
            return complete(this);
        }

        return incomplete(new Follower<>(getId(), appendEntriesRequest.getTerm(),null, getLog(), getCluster()));
    }

    @Override
    protected Result<ID> handle(RequestVoteResponse<ID> requestVoteResponse) {
        if (messageIsNotStale(requestVoteResponse) &&
                requestVoteResponse.isVoteGranted()) {
            return recordVoteAndClaimLeadershipIfEligible(requestVoteResponse.getSource());
        }
        return complete(this);
    }

    @Override
    public ServerStateType getServerStateType() {
        return CANDIDATE;
    }

    public Result<ID> recordVoteAndClaimLeadershipIfEligible(ID voter) {
        this.receivedVotes.add(voter);
        if (getCluster().isQuorum(getReceivedVotes())) {
            Leader<ID> leaderState = new Leader<>(getId(), getCurrentTerm(), getLog(), getCluster(), new ClientRegistryFactory<>(), new LogReplicatorFactory<>());
            leaderState.sendHeartbeatMessage();
            return complete(leaderState);
        } else {
            return complete(this);
        }
    }

    public void requestVotes() {
        getCluster().send(new RequestVoteRequest<>(getCurrentTerm(), getId(), getLog().getLastLogIndex(), getLog().getLastLogTerm()));
    }

    public Set<ID> getReceivedVotes() {
        return unmodifiableSet(receivedVotes);
    }
}
