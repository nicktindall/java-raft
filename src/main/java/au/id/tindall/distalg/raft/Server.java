package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.ServerState.CANDIDATE;
import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;

import java.io.Serializable;
import java.util.Optional;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogSummary;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;

public class Server<ID extends Serializable> {

    private final ID id;
    private final Cluster<ID> cluster;
    private Term currentTerm;
    private ServerState state;
    private ID votedFor;
    private Log log;

    public Server(ID id, Cluster<ID> cluster) {
        this(id, new Term(0), null, new Log(), cluster);
    }

    public Server(ID id, Term currentTerm, ID votedFor, Log log, Cluster<ID> cluster) {
        this.id = id;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
        this.cluster = cluster;
        state = FOLLOWER;
    }

    public void electionTimeout() {
        state = CANDIDATE;
        currentTerm = currentTerm.next();
        votedFor = id;
        cluster.broadcastMessage(new RequestVoteRequest<>(currentTerm, id, log.getLastLogIndex(), log.getLastLogTerm()));
    }

    public RequestVoteResponse handle(RequestVoteRequest<ID> requestVote) {
        if (requestVote.getTerm().isLessThan(currentTerm)) {
            return new RequestVoteResponse(currentTerm, false);
        }

        updateTerm(requestVote.getTerm());

        boolean grantVote = haveNotVotedOrHaveAlreadyVotedForCandidate(requestVote)
                && candidatesLogIsAtLeastUpToDateAsMine(requestVote);
        if (grantVote) {
            votedFor = requestVote.getCandidateId();
        }
        return new RequestVoteResponse(currentTerm, grantVote);
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
}
