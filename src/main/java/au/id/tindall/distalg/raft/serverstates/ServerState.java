package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.Optional.empty;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.RpcMessage;

public class ServerState<ID extends Serializable> {

    private final ID id;
    private final Cluster<ID> cluster;
    private Term currentTerm;
    private final ServerStateType serverStateType;
    private ID votedFor;
    private Log log;
    private Set<ID> receivedVotes;
    private int commitIndex;

    public ServerState(ID id, Term currentTerm, ServerStateType serverStateType, ID votedFor, Log log, Cluster<ID> cluster) {
        this.id = id;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
        this.cluster = cluster;
        this.commitIndex = 0;
        this.receivedVotes = new HashSet<>();
        this.serverStateType = serverStateType;
    }

    public void requestVotes() {
        cluster.send(new RequestVoteRequest<>(currentTerm, id, log.getLastLogIndex(), log.getLastLogTerm()));
    }

    public Result<ID> handle(RpcMessage<ID> message) {
        if (message instanceof RequestVoteRequest) {
            handle((RequestVoteRequest<ID>) message);
        } else if (message instanceof RequestVoteResponse) {
            return handle((RequestVoteResponse<ID>) message);
        } else if (message instanceof AppendEntriesRequest) {
            return handle((AppendEntriesRequest<ID>) message);
        } else if (message instanceof AppendEntriesResponse) {
            handle((AppendEntriesResponse<ID>) message);
        } else {
            throw new UnsupportedOperationException(format("No overload for message type %s", message.getClass().getName()));
        }
        return new Result<>(true, this);
    }

    public Result<ID> handle(AppendEntriesRequest<ID> appendEntriesRequest) {
        if (appendEntriesRequest.getTerm().isLessThan(currentTerm)) {
            cluster.send(new AppendEntriesResponse<>(currentTerm, id, appendEntriesRequest.getLeaderId(), false, empty()));
            return complete(this);
        }

        // become follower if candidate
        if (serverStateType.equals(CANDIDATE)) {
            return incomplete(new ServerState<>(id, currentTerm, FOLLOWER, null, log, cluster));
        }

        if (appendEntriesRequest.getPrevLogIndex() > 0 &&
                !log.containsPreviousEntry(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm())) {
            cluster.send(new AppendEntriesResponse<>(currentTerm, id, appendEntriesRequest.getLeaderId(), false, empty()));
            return complete(this);
        }

        log.appendEntries(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getEntries());
        commitIndex = min(log.getLastLogIndex(), appendEntriesRequest.getLeaderCommit());
        int indexOfLastEntryAppended = appendEntriesRequest.getPrevLogIndex() + appendEntriesRequest.getEntries().size();
        cluster.send(new AppendEntriesResponse<>(currentTerm, id, appendEntriesRequest.getLeaderId(), true, Optional.of(indexOfLastEntryAppended)));
        return complete(this);
    }

    public void handle(AppendEntriesResponse<ID> appendEntriesResponse) {
        // Only leaders are interested in these
    }

    public void handle(RequestVoteRequest<ID> requestVote) {
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

    public Result<ID> handle(RequestVoteResponse<ID> requestVoteResponse) {
        if (!responseIsStale(requestVoteResponse.getTerm())) {
            if (requestVoteResponse.isVoteGranted()) {
                return recordVoteAndClaimLeadershipIfEligible(requestVoteResponse.getSource());
            }
        }
        return complete(this);
    }

    public Result<ID> recordVoteAndClaimLeadershipIfEligible(ID voter) {
        this.receivedVotes.add(voter);
        if (serverStateType == CANDIDATE && cluster.isQuorum(getReceivedVotes())) {
            Leader<ID> leaderState = new Leader<>(id, currentTerm, null, log, cluster);
            leaderState.sendHeartbeatMessage();
            return complete(leaderState);
        } else {
            return complete(this);
        }
    }

    protected boolean responseIsStale(Term responseTerm) {
        return responseTerm.isLessThan(currentTerm);
    }

    private boolean candidatesLogIsAtLeastUpToDateAsMine(RequestVoteRequest<ID> requestVote) {
        return log.getSummary().compareTo(new LogSummary(requestVote.getLastLogTerm(), requestVote.getLastLogIndex())) <= 0;
    }

    private boolean haveNotVotedOrHaveAlreadyVotedForCandidate(RequestVoteRequest<ID> requestVote) {
        return votedFor == null || votedFor.equals(requestVote.getCandidateId());
    }

    public ID getId() {
        return id;
    }

    public Term getCurrentTerm() {
        return currentTerm;
    }

    public ServerStateType getServerStateType() {
        return serverStateType;
    }

    public Optional<ID> getVotedFor() {
        return Optional.ofNullable(votedFor);
    }

    public Log getLog() {
        return log;
    }

    public Cluster<ID> getCluster() {
        return cluster;
    }

    public Set<ID> getReceivedVotes() {
        return unmodifiableSet(receivedVotes);
    }

    public int getCommitIndex() {
        return commitIndex;
    }
}
