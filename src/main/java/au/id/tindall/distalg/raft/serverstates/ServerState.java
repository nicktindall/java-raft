package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static java.lang.String.format;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

public abstract class ServerState<ID extends Serializable> {

    private final Cluster<ID> cluster;
    private Term currentTerm;
    private ID votedFor;
    private Log log;
    protected final ServerStateFactory<ID> serverStateFactory;
    private ID currentLeader;

    public ServerState(Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory, ID currentLeader) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
        this.cluster = cluster;
        this.serverStateFactory = serverStateFactory;
        this.currentLeader = currentLeader;
    }

    public CompletableFuture<? extends ClientResponseMessage> handle(ClientRequestMessage<ID> message) {
        if (message instanceof RegisterClientRequest) {
            return handle((RegisterClientRequest<ID>) message);
        } else if (message instanceof ClientRequestRequest) {
            return handle(((ClientRequestRequest<ID>) message));
        } else {
            throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        }
    }

    public Result<ID> handle(RpcMessage<ID> message) {
        if (message.getTerm().isGreaterThan(currentTerm)) {
            return incomplete(serverStateFactory.createFollower(message.getTerm(), message.getSource()));
        }

        if (message instanceof RequestVoteRequest) {
            return handle((RequestVoteRequest<ID>) message);
        } else if (message instanceof RequestVoteResponse) {
            return handle((RequestVoteResponse<ID>) message);
        } else if (message instanceof AppendEntriesRequest) {
            return handle((AppendEntriesRequest<ID>) message);
        } else if (message instanceof AppendEntriesResponse) {
            return handle((AppendEntriesResponse<ID>) message);
        } else {
            throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        }
    }

    protected CompletableFuture<ClientRequestResponse<ID>> handle(ClientRequestRequest<ID> clientRequestRequest) {
        return CompletableFuture.completedFuture(new ClientRequestResponse<>(ClientRequestStatus.NOT_LEADER, null, currentLeader));
    }

    protected CompletableFuture<RegisterClientResponse<ID>> handle(RegisterClientRequest<ID> registerClientRequest) {
        return CompletableFuture.completedFuture(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, currentLeader));
    }

    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        return complete(this);
    }

    protected Result<ID> handle(AppendEntriesResponse<ID> appendEntriesResponse) {
        return complete(this);
    }

    protected Result<ID> handle(RequestVoteRequest<ID> requestVote) {
        if (requestVote.getTerm().isLessThan(currentTerm)) {
            cluster.sendRequestVoteResponse(currentTerm, requestVote.getCandidateId(), false);
        } else {
            boolean grantVote = haveNotVotedOrHaveAlreadyVotedForCandidate(requestVote)
                    && candidatesLogIsAtLeastUpToDateAsMine(requestVote);
            if (grantVote) {
                votedFor = requestVote.getCandidateId();
            }
            cluster.sendRequestVoteResponse(currentTerm, requestVote.getCandidateId(), grantVote);
        }
        return complete(this);
    }

    protected Result<ID> handle(RequestVoteResponse<ID> requestVoteResponse) {
        return complete(this);
    }

    private boolean candidatesLogIsAtLeastUpToDateAsMine(RequestVoteRequest<ID> requestVote) {
        return log.getSummary().compareTo(new LogSummary(requestVote.getLastLogTerm(), requestVote.getLastLogIndex())) <= 0;
    }

    private boolean haveNotVotedOrHaveAlreadyVotedForCandidate(RequestVoteRequest<ID> requestVote) {
        return votedFor == null || votedFor.equals(requestVote.getCandidateId());
    }

    protected boolean messageIsStale(RpcMessage<ID> message) {
        return message.getTerm().isLessThan(currentTerm);
    }

    protected boolean messageIsNotStale(RpcMessage<ID> message) {
        return !messageIsStale(message);
    }

    public Term getCurrentTerm() {
        return currentTerm;
    }

    public abstract ServerStateType getServerStateType();

    public Optional<ID> getVotedFor() {
        return Optional.ofNullable(votedFor);
    }

    public Log getLog() {
        return log;
    }

    public Cluster<ID> getCluster() {
        return cluster;
    }

    public void dispose() {
    }
}
