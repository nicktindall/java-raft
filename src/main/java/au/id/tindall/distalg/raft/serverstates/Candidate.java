package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.cluster.Configuration;
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

public class Candidate<I extends Serializable> extends ServerStateImpl<I> {

    private static final long WARNING_THRESHOLD_MS = 30;
    private static final Logger LOGGER = getLogger();

    private final Set<I> receivedVotes;
    private final ElectionScheduler electionScheduler;
    private final Configuration<I> configuration;

    public Candidate(PersistentState<I> persistentState, Log log, Cluster<I> cluster, ServerStateFactory<I> serverStateFactory, ElectionScheduler electionScheduler, Configuration<I> configuration) {
        super(persistentState, log, cluster, serverStateFactory, null, electionScheduler);
        this.electionScheduler = electionScheduler;
        this.configuration = configuration;
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
    protected Result<I> handle(AppendEntriesRequest<I> appendEntriesRequest) {
        if (messageIsStale(appendEntriesRequest)) {
            // we reply using the sender's term to avoid them following us before the election is decided
            cluster.sendAppendEntriesResponse(appendEntriesRequest.getTerm(), appendEntriesRequest.getLeaderId(), false, empty());
            return complete(this);
        }

        persistentState.setCurrentTerm(appendEntriesRequest.getTerm());
        return incomplete(serverStateFactory.createFollower(appendEntriesRequest.getLeaderId()));
    }

    @Override
    protected Result<I> handle(RequestVoteResponse<I> requestVoteResponse) {
        if (messageIsNotStale(requestVoteResponse) &&
                requestVoteResponse.isVoteGranted()) {
            return complete(recordVoteAndClaimLeadershipIfEligible(requestVoteResponse.getSource()));
        }
        return complete(this);
    }

    @Override
    protected Result<I> handle(TimeoutNowMessage<I> timeoutNowMessage) {
        long startTime = System.currentTimeMillis();
        persistentState.setCurrentTermAndVotedFor(persistentState.getCurrentTerm().next(), persistentState.getId());
        long setPersistentStateDuration = System.currentTimeMillis() - startTime;
        ServerState<I> nextState = recordVoteAndClaimLeadershipIfEligible(persistentState.getId());
        electionScheduler.resetTimeout();
        long requestVotesStart = System.currentTimeMillis();
        nextState.requestVotes(timeoutNowMessage.isEarlyElection());
        long requestVotesDuration = System.currentTimeMillis() - requestVotesStart;
        long totalDuration = System.currentTimeMillis() - startTime;
        if (totalDuration > WARNING_THRESHOLD_MS) {
            LOGGER.warn("Handling TimeoutNowMessage took {}ms, (expected < {}ms, setPersistentStateDuration={}ms, requestVotesDuration={}ms)",
                    totalDuration, WARNING_THRESHOLD_MS, setPersistentStateDuration, requestVotesDuration);
        }
        return complete(nextState);
    }

    @Override
    public ServerStateType getServerStateType() {
        return CANDIDATE;
    }

    @Override
    public void requestVotes(boolean earlyElection) {
        cluster.sendRequestVoteRequest(configuration, persistentState.getCurrentTerm(), log.getLastLogIndex(), log.getLastLogTerm(), earlyElection);
    }

    public ServerState<I> recordVoteAndClaimLeadershipIfEligible(I voter) {
        this.receivedVotes.add(voter);
        if (configuration.isQuorum(getReceivedVotes())) {
            return serverStateFactory.createLeader();
        } else {
            return this;
        }
    }

    public Set<I> getReceivedVotes() {
        return unmodifiableSet(receivedVotes);
    }
}
