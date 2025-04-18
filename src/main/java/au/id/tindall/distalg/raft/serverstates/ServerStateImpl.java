package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.rpc.clustermembership.AbdicateLeadershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AbdicateLeadershipResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotResponse;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static java.lang.String.format;
import static org.apache.logging.log4j.LogManager.getLogger;

public abstract class ServerStateImpl<I> implements ServerState<I> {

    private static final Logger LOGGER = getLogger();

    protected final PersistentState<I> persistentState;
    protected final Cluster<I> cluster;
    protected final Log log;
    protected final ServerStateFactory<I> serverStateFactory;
    protected final I currentLeader;
    protected final ElectionScheduler electionScheduler;

    protected ServerStateImpl(PersistentState<I> persistentState, Log log, Cluster<I> cluster, ServerStateFactory<I> serverStateFactory, I currentLeader, ElectionScheduler electionScheduler) {
        this.persistentState = persistentState;
        this.log = log;
        this.cluster = cluster;
        this.serverStateFactory = serverStateFactory;
        this.currentLeader = currentLeader;
        this.electionScheduler = electionScheduler;
    }

    private boolean leaderHeartbeatIsCurrent() {
        return electionScheduler.isHeartbeatCurrent();
    }

    @Override
    public Result<I> handle(RpcMessage<I> message) {
        if (message.getTerm().isGreaterThan(persistentState.getCurrentTerm())) {
            if (message instanceof RequestVoteRequest<I> voteRequest && leaderHeartbeatIsCurrent() && !voteRequest.isEarlyElection()) {
                LOGGER.debug("Got vote request from {} while leader ({}) is active, rejecting", message.getSource(), currentLeader);
                cluster.sendRequestVoteResponse(persistentState.getCurrentTerm(), message.getSource(), false);
                return complete(this);
            } else {
                LOGGER.debug("Received a message from a future term, transitioning from {} to {} state, message={}",
                        getServerStateType(), ServerStateType.FOLLOWER, message);
                persistentState.setCurrentTerm(message.getTerm());
                return incomplete(serverStateFactory.createFollower(message.getSource()));
            }
        }

        if (message instanceof RequestVoteRequest) {
            return handle((RequestVoteRequest<I>) message);
        } else if (message instanceof RequestVoteResponse) {
            return handle((RequestVoteResponse<I>) message);
        } else if (message instanceof AppendEntriesRequest) {
            return handle((AppendEntriesRequest<I>) message);
        } else if (message instanceof AppendEntriesResponse) {
            return handle((AppendEntriesResponse<I>) message);
        } else if (message instanceof TimeoutNowMessage) {
            return handle((TimeoutNowMessage<I>) message);
        } else if (message instanceof TransferLeadershipMessage) {
            return handle((TransferLeadershipMessage<I>) message);
        } else if (message instanceof InstallSnapshotRequest) {
            return handle((InstallSnapshotRequest<I>) message);
        } else if (message instanceof InstallSnapshotResponse) {
            return handle((InstallSnapshotResponse<I>) message);
        } else {
            throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        }
    }

    public void requestVotes(boolean earlyElection) {
        // Do nothing by default, only candidates request votes
    }

    protected CompletableFuture<ClientRequestResponse<I>> handle(ClientRequestRequest<I> clientRequestRequest) {
        return CompletableFuture.completedFuture(new ClientRequestResponse<>(ClientRequestStatus.NOT_LEADER, null, currentLeader));
    }

    protected CompletableFuture<RegisterClientResponse<I>> handle(RegisterClientRequest<I> registerClientRequest) {
        return CompletableFuture.completedFuture(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, currentLeader));
    }

    protected Result<I> handle(AppendEntriesRequest<I> appendEntriesRequest) {
        return complete(this);
    }

    protected Result<I> handle(AppendEntriesResponse<I> appendEntriesResponse) {
        return complete(this);
    }

    protected Result<I> handle(RequestVoteRequest<I> requestVote) {
        if (messageIsStale(requestVote)) {
            // we reply using the sender's term in case we're a candidate, and they interpret the response as an election victory
            LOGGER.debug("Rejecting stale vote request from {} (requestTerm={}, myTerm={})",
                    requestVote.getCandidateId(), requestVote.getTerm(), persistentState.getCurrentTerm());
            cluster.sendRequestVoteResponse(requestVote.getTerm(), requestVote.getCandidateId(), false);
        } else {
            final boolean haveNotVotedOrHaveAlreadyVotedForCandidate = haveNotVotedOrHaveAlreadyVotedForCandidate(requestVote);
            boolean grantVote = haveNotVotedOrHaveAlreadyVotedForCandidate && candidatesLogIsAtLeastUpToDateAsMine(requestVote);
            if (grantVote) {
                persistentState.setVotedFor(requestVote.getCandidateId());
            }
            LOGGER.debug("Responding to vote request from {} (requestTerm={}, myTerm={}, heartbeatIsCurrent={}, earlyElection={}, haveNotVotedOrHaveAlreadyVotedForCandidate={}, granted={})",
                    requestVote.getCandidateId(), requestVote.getTerm(), persistentState.getCurrentTerm(), leaderHeartbeatIsCurrent(), requestVote.isEarlyElection(), haveNotVotedOrHaveAlreadyVotedForCandidate, grantVote);
            cluster.sendRequestVoteResponse(persistentState.getCurrentTerm(), requestVote.getCandidateId(), grantVote);
        }
        return complete(this);
    }

    protected Result<I> handle(RequestVoteResponse<I> requestVoteResponse) {
        return complete(this);
    }

    protected Result<I> handle(InstallSnapshotRequest<I> installSnapshotRequest) {
        return complete(this);
    }

    protected Result<I> handle(InstallSnapshotResponse<I> installSnapshotResponse) {
        return complete(this);
    }

    protected Result<I> handle(TimeoutNowMessage<I> timeoutNowMessage) {
        return incomplete(serverStateFactory.createCandidate());
    }

    protected Result<I> handle(TransferLeadershipMessage<I> transferLeadershipMessage) {
        return complete(this);
    }

    private boolean candidatesLogIsAtLeastUpToDateAsMine(RequestVoteRequest<I> requestVote) {
        return log.getSummary().compareTo(new LogSummary(requestVote.getLastLogTerm(), requestVote.getLastLogIndex())) <= 0;
    }

    private boolean haveNotVotedOrHaveAlreadyVotedForCandidate(RequestVoteRequest<I> requestVote) {
        Optional<I> votedFor = persistentState.getVotedFor();
        return votedFor.isEmpty() || votedFor.get().equals(requestVote.getCandidateId());
    }

    protected boolean messageIsStale(RpcMessage<I> message) {
        return message.getTerm().isLessThan(persistentState.getCurrentTerm());
    }

    protected boolean messageIsNotStale(RpcMessage<I> message) {
        return !messageIsStale(message);
    }

    @Override
    public abstract ServerStateType getServerStateType();

    @Override
    public Log getLog() {
        return log;
    }

    @Override
    public void enterState() {
    }

    @Override
    public void leaveState() {
    }

    @Override
    public <R extends ClientResponseMessage<I>> CompletableFuture<R> handle(ClientRequestMessage<I, R> message) {
        return switch (message) {
            case AddServerRequest addServerRequest ->
                    (CompletableFuture<R>) this.handle((AddServerRequest<I>) addServerRequest);
            case RemoveServerRequest removeServerRequest ->
                    (CompletableFuture<R>) this.handle((RemoveServerRequest<I>) removeServerRequest);
            case ClientRequestRequest clientRequestRequest ->
                    (CompletableFuture<R>) this.handle((ClientRequestRequest<I>) clientRequestRequest);
            case RegisterClientRequest registerClientRequest ->
                    (CompletableFuture<R>) this.handle((RegisterClientRequest<I>) registerClientRequest);
            case AbdicateLeadershipRequest abdicateLeadershipRequest ->
                    (CompletableFuture<R>) this.handle((AbdicateLeadershipRequest<I>) abdicateLeadershipRequest);
            default ->
                    throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        };
    }

    protected CompletableFuture<AddServerResponse<I>> handle(AddServerRequest<I> addServerRequest) {
        return CompletableFuture.completedFuture(AddServerResponse.getNotLeader());
    }

    protected CompletableFuture<RemoveServerResponse<I>> handle(RemoveServerRequest<I> removeServerRequest) {
        return CompletableFuture.completedFuture(RemoveServerResponse.getNotLeader());
    }

    protected CompletableFuture<AbdicateLeadershipResponse<I>> handle(AbdicateLeadershipRequest<I> abdicateLeadershipRequest) {
        return CompletableFuture.completedFuture(AbdicateLeadershipResponse.getNotLeader());
    }
}
