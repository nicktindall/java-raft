package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
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
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.ClusterMembershipResponse;
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

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static java.lang.String.format;

public abstract class ServerState<ID extends Serializable> {

    protected final PersistentState<ID> persistentState;
    protected final Cluster<ID> cluster;
    protected final Log log;
    protected final ServerStateFactory<ID> serverStateFactory;
    protected final ID currentLeader;

    public ServerState(PersistentState<ID> persistentState, Log log, Cluster<ID> cluster, ServerStateFactory<ID> serverStateFactory, ID currentLeader) {
        this.persistentState = persistentState;
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
        if (message.getTerm().isGreaterThan(persistentState.getCurrentTerm())) {
            persistentState.setCurrentTerm(message.getTerm());
            return incomplete(serverStateFactory.createFollower(message.getSource()));
        }

        if (message instanceof RequestVoteRequest) {
            return handle((RequestVoteRequest<ID>) message);
        } else if (message instanceof RequestVoteResponse) {
            return handle((RequestVoteResponse<ID>) message);
        } else if (message instanceof AppendEntriesRequest) {
            return handle((AppendEntriesRequest<ID>) message);
        } else if (message instanceof AppendEntriesResponse) {
            return handle((AppendEntriesResponse<ID>) message);
        } else if (message instanceof TimeoutNowMessage) {
            return handle((TimeoutNowMessage<ID>) message);
        } else if (message instanceof TransferLeadershipMessage) {
            return handle((TransferLeadershipMessage<ID>) message);
        } else if (message instanceof InstallSnapshotRequest) {
            return handle((InstallSnapshotRequest<ID>) message);
        } else if (message instanceof InstallSnapshotResponse) {
            return handle((InstallSnapshotResponse<ID>) message);
        } else {
            throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        }
    }

    protected void requestVotes() {
        // Do nothing by default, only candidates request votes
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
        if (requestVote.getTerm().isLessThan(persistentState.getCurrentTerm())) {
            cluster.sendRequestVoteResponse(persistentState.getCurrentTerm(), requestVote.getCandidateId(), false);
        } else {
            boolean grantVote = haveNotVotedOrHaveAlreadyVotedForCandidate(requestVote)
                    && candidatesLogIsAtLeastUpToDateAsMine(requestVote);
            if (grantVote) {
                persistentState.setVotedFor(requestVote.getCandidateId());
            }
            cluster.sendRequestVoteResponse(persistentState.getCurrentTerm(), requestVote.getCandidateId(), grantVote);
        }
        return complete(this);
    }

    protected Result<ID> handle(RequestVoteResponse<ID> requestVoteResponse) {
        return complete(this);
    }

    protected Result<ID> handle(InstallSnapshotRequest<ID> installSnapshotRequest) {
        return complete(this);
    }

    protected Result<ID> handle(InstallSnapshotResponse<ID> installSnapshotResponse) {
        return complete(this);
    }

    protected Result<ID> handle(TimeoutNowMessage<ID> timeoutNowMessage) {
        return incomplete(serverStateFactory.createCandidate());
    }

    protected Result<ID> handle(TransferLeadershipMessage<ID> transferLeadershipMessage) {
        return complete(this);
    }

    private boolean candidatesLogIsAtLeastUpToDateAsMine(RequestVoteRequest<ID> requestVote) {
        return log.getSummary().compareTo(new LogSummary(requestVote.getLastLogTerm(), requestVote.getLastLogIndex())) <= 0;
    }

    private boolean haveNotVotedOrHaveAlreadyVotedForCandidate(RequestVoteRequest<ID> requestVote) {
        Optional<ID> votedFor = persistentState.getVotedFor();
        return votedFor.isEmpty() || votedFor.get().equals(requestVote.getCandidateId());
    }

    protected boolean messageIsStale(RpcMessage<ID> message) {
        return message.getTerm().isLessThan(persistentState.getCurrentTerm());
    }

    protected boolean messageIsNotStale(RpcMessage<ID> message) {
        return !messageIsStale(message);
    }

    public abstract ServerStateType getServerStateType();

    public Log getLog() {
        return log;
    }

    public void enterState() {
    }

    public void leaveState() {
    }

    public CompletableFuture<? extends ClusterMembershipResponse> handle(ClusterMembershipRequest<ID> message) {
        if (message instanceof AddServerRequest) {
            return this.handle((AddServerRequest<ID>) message);
        } else if (message instanceof RemoveServerRequest) {
            return this.handle((RemoveServerRequest<ID>) message);
        } else {
            throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        }
    }

    protected CompletableFuture<AddServerResponse> handle(AddServerRequest<ID> addServerRequest) {
        return CompletableFuture.completedFuture(AddServerResponse.NOT_LEADER);
    }

    protected CompletableFuture<RemoveServerResponse> handle(RemoveServerRequest<ID> removeServerRequest) {
        return CompletableFuture.completedFuture(RemoveServerResponse.NOT_LEADER);
    }
}
