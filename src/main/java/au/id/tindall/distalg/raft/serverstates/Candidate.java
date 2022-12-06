package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.state.PersistentState;
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

    public Candidate(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory, ElectionScheduler<ID> electionScheduler) {
        super(persistentState, log, cluster, serverStateFactory, null);
        this.electionScheduler = electionScheduler;
        receivedVotes = new HashSet<>();
    }

    @Override
    public void enterState() {
        LOGGER.debug("Server entering Candidate state (term={}, lastIndex={}, lastTerm={})",
                persistentState.getCurrentTerm().getNumber() + 1,   // Candidate will increment its term before it requests votes
                log.getLastLogIndex(),
                log.getLastLogTerm());
        this.electionScheduler.startTimeouts();
    }

    @Override
    public void leaveState() {
        this.electionScheduler.stopTimeouts();
    }

    @Override
    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (messageIsStale(appendEntriesRequest)) {
            // we reply using the sender's term to avoid them following us before the election is decided
            cluster.sendAppendEntriesResponse(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        persistentState.setCurrentTerm(appendEntriesRequest.getTerm());
        return incomplete(serverStateFactory.createFollower(appendEntriesRequest.getLeaderId()));
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
    protected Result<ID> handle(TimeoutNowMessage<ID> timeoutNowMessage) {
        persistentState.setCurrentTerm(persistentState.getCurrentTerm().next());
        persistentState.setVotedFor(persistentState.getId());
        ServerState<ID> nextState = recordVoteAndClaimLeadershipIfEligible(persistentState.getId());
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
        cluster.sendRequestVoteRequest(persistentState.getCurrentTerm(), log.getLastLogIndex(), log.getLastLogTerm());
    }

    public ServerState<ID> recordVoteAndClaimLeadershipIfEligible(ID voter) {
        this.receivedVotes.add(voter);
        if (cluster.isQuorum(getReceivedVotes())) {
            return serverStateFactory.createLeader();
        } else {
            return this;
        }
    }

    public Set<ID> getReceivedVotes() {
        return unmodifiableSet(receivedVotes);
    }
}
