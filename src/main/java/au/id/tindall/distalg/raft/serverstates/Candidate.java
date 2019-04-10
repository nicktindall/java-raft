package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static java.util.Collections.unmodifiableSet;
import static java.util.Optional.empty;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;

public class Candidate<ID extends Serializable> extends ServerState<ID> {

    private final Set<ID> receivedVotes;

    public Candidate(Term currentTerm, Log log, Cluster<ID> cluster, ID ownId, ServerStateFactory<ID> serverStateFactory) {
        super(currentTerm, ownId, log, cluster, serverStateFactory);
        receivedVotes = new HashSet<>();
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (messageIsStale(appendEntriesRequest)) {
            getCluster().sendAppendEntriesResponse(getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        return incomplete(serverStateFactory.createFollower(appendEntriesRequest.getTerm()));
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
            Leader<ID> leaderState = serverStateFactory.createLeader(getCurrentTerm());
            leaderState.sendHeartbeatMessage();
            return complete(leaderState);
        } else {
            return complete(this);
        }
    }

    public void requestVotes() {
        getCluster().sendRequestVoteRequest(getCurrentTerm(), getLog().getLastLogIndex(), getLog().getLastLogTerm());
    }

    public Set<ID> getReceivedVotes() {
        return unmodifiableSet(receivedVotes);
    }
}
