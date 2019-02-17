package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static java.lang.String.format;

import java.io.Serializable;
import java.util.Optional;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.RpcMessage;

public abstract class ServerState<ID extends Serializable> {

    private final ID id;
    private final Cluster<ID> cluster;
    private Term currentTerm;
    private ID votedFor;
    private Log log;
    private int commitIndex;

    public ServerState(ID id, Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster) {
        this.id = id;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
        this.cluster = cluster;
        this.commitIndex = 0;
    }

    public Result<ID> handle(RpcMessage<ID> message) {
        if (message instanceof RequestVoteRequest) {
            handle((RequestVoteRequest<ID>) message);
        } else if (message instanceof RequestVoteResponse) {
            return handle((RequestVoteResponse<ID>) message);
        } else if (message instanceof AppendEntriesRequest) {
            return handle((AppendEntriesRequest<ID>) message);
        } else if (message instanceof AppendEntriesResponse) {
            return handle((AppendEntriesResponse<ID>) message);
        } else {
            throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        }
        return new Result<>(true, this);
    }

    protected Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        return complete(this);
    }

    protected Result<ID> handle(AppendEntriesResponse<ID> appendEntriesResponse) {
        return complete(this);
    }

    protected void handle(RequestVoteRequest<ID> requestVote) {
        if (requestVote.getTerm().isLessThan(currentTerm)) {
            cluster.send(new RequestVoteResponse<>(currentTerm, id, requestVote.getCandidateId(), false));
        } else {
            boolean grantVote = haveNotVotedOrHaveAlreadyVotedForCandidate(requestVote)
                    && candidatesLogIsAtLeastUpToDateAsMine(requestVote);
            if (grantVote) {
                votedFor = requestVote.getCandidateId();
            }
            cluster.send(new RequestVoteResponse<>(currentTerm, id, requestVote.getCandidateId(), grantVote));
        }
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

    public ID getId() {
        return id;
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

    public int getCommitIndex() {
        return commitIndex;
    }

    protected void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }
}
