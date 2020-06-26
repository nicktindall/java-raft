package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.driver.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.InitiateElectionMessage;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static java.util.Collections.unmodifiableSet;
import static java.util.Optional.empty;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Candidate<ID extends Serializable> extends ServerState<ID> {

    private static final Logger LOGGER = getLogger();

    private final Set<ID> receivedVotes;
    private final ElectionScheduler<ID> electionScheduler;

    public Candidate(Term currentTerm, Log log, Cluster<ID> cluster, ID ownId, ServerStateFactory<ID> serverStateFactory, ElectionScheduler<ID> electionScheduler) {
        super(currentTerm, ownId, log, cluster, serverStateFactory, null);
        this.electionScheduler = electionScheduler;
        receivedVotes = new HashSet<>();
    }

    @Override
    public void enterState() {
        LOGGER.debug("Server entering Candidate state");
        this.electionScheduler.startTimeouts();
    }

    @Override
    public void leaveState() {
        this.electionScheduler.stopTimeouts();
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (messageIsStale(appendEntriesRequest)) {
            getCluster().sendAppendEntriesResponse(getCurrentTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        return incomplete(serverStateFactory.createFollower(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId()));
    }

    @Override
    protected Result<ID> handle(RequestVoteResponse<ID> requestVoteResponse) {
        if (messageIsNotStale(requestVoteResponse) &&
                requestVoteResponse.isVoteGranted()) {
            return complete(recordVoteAndClaimLeadershipIfEligible(requestVoteResponse.getSource()));
        }
        return complete(this);
    }

    @Override
    protected Result<ID> handle(InitiateElectionMessage<ID> initiateElectionMessage) {
        ServerState<ID> nextState = recordVoteAndClaimLeadershipIfEligible(initiateElectionMessage.getSource());
        electionScheduler.resetTimeout();
        nextState.requestVotes();
        return complete(nextState);
    }

    @Override
    public ServerStateType getServerStateType() {
        return CANDIDATE;
    }

    @Override
    protected void requestVotes() {
        getCluster().sendRequestVoteRequest(getCurrentTerm(), getLog().getLastLogIndex(), getLog().getLastLogTerm());
    }

    public ServerState<ID> recordVoteAndClaimLeadershipIfEligible(ID voter) {
        this.receivedVotes.add(voter);
        if (getCluster().isQuorum(getReceivedVotes())) {
            return serverStateFactory.createLeader(getCurrentTerm());
        } else {
            return this;
        }
    }

    public Set<ID> getReceivedVotes() {
        return unmodifiableSet(receivedVotes);
    }
}
