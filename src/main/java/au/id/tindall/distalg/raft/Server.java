package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.ServerState.CANDIDATE;
import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;
import static au.id.tindall.distalg.raft.ServerState.LEADER;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;

public class Server<ID extends Serializable> {

    private final ID id;
    private final Cluster<ID> cluster;
    private Term currentTerm;
    private ServerState state;
    private ID votedFor;
    private Log log;
    private Set<ID> receivedVotes;
    private int commitIndex;

    public Server(ID id, Cluster<ID> cluster) {
        this(id, new Term(0), null, new Log(), cluster);
    }

    public Server(ID id, Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster) {
        this.id = id;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
        this.cluster = cluster;
        this.commitIndex = 0;
        this.receivedVotes = new HashSet<>();
        state = FOLLOWER;
    }

    public void electionTimeout() {
        state = CANDIDATE;
        currentTerm = currentTerm.next();
        receivedVotes = new HashSet<>();
        votedFor = null;
        cluster.send(new RequestVoteRequest<>(currentTerm, id, log.getLastLogIndex(), log.getLastLogTerm()));
    }

    public void handle(RequestVoteRequest<ID> requestVote) {
        updateTerm(requestVote.getTerm());

        if (requestVote.getTerm().isLessThan(currentTerm)) {
            cluster.send(new RequestVoteResponse<>(id, requestVote.getCandidateId(), currentTerm, false));
        } else {
            boolean grantVote = haveNotVotedOrHaveAlreadyVotedForCandidate(requestVote)
                    && candidatesLogIsAtLeastUpToDateAsMine(requestVote);
            if (grantVote) {
            votedFor = requestVote.getCandidateId();
            }
            cluster.send(new RequestVoteResponse<>(id, requestVote.getCandidateId(), currentTerm, grantVote));
        }
    }

    public void handle(RequestVoteResponse<ID> requestVoteResponse) {
        updateTerm(requestVoteResponse.getTerm());

        if (responseIsStale(requestVoteResponse.getTerm())) {
            return;
        }

        if (requestVoteResponse.isVoteGranted()) {
            recordVoteAndClaimLeadershipIfEligible(requestVoteResponse.getSource());
        }
    }

    private void recordVoteAndClaimLeadershipIfEligible(ID voter) {
        this.receivedVotes.add(voter);
        if (state == CANDIDATE && cluster.isQuorum(getReceivedVotes())) {
            state = LEADER;
            sendHeartbeatMessage();
        }
    }

    private void sendHeartbeatMessage() {
        cluster.send(new AppendEntriesRequest<>(currentTerm, id, log.getLastLogIndex(), log.getLastLogTerm(), emptyList(), commitIndex));
    }

    private boolean responseIsStale(Term responseTerm) {
        return responseTerm.isLessThan(currentTerm);
    }

    private void updateTerm(Term rpcTerm) {
        if (rpcTerm.isGreaterThan(currentTerm)) {
            state = FOLLOWER;
            currentTerm = rpcTerm;
            votedFor = null;
        }
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

    public ServerState getState() {
        return state;
    }

    public Optional<ID> getVotedFor() {
        return Optional.ofNullable(votedFor);
    }

    public Log getLog() {
        return log;
    }

    public Set<ID> getReceivedVotes() {
        return unmodifiableSet(receivedVotes);
    }
}
