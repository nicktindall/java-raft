package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;
import java.util.Optional;

import au.id.tindall.distalg.raft.log.Term;

public class RequestVoteRequest<ID extends Serializable> implements Serializable {

    private final Term term;
    private final ID candidateId;
    private final int lastLogIndex;
    private final Term lastLogTerm;

    public RequestVoteRequest(Term term, ID candidateId, int lastLogIndex, Optional<Term> lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm.orElse(null);
    }

    public Term getTerm() {
        return term;
    }

    public ID getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public Optional<Term> getLastLogTerm() {
        return Optional.ofNullable(lastLogTerm);
    }

    @Override
    public String toString() {
        return "RequestVoteRequest{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
