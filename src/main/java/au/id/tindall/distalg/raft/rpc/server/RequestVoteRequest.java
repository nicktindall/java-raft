package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;
import java.util.Optional;

public class RequestVoteRequest<I extends Serializable> extends BroadcastMessage<I> {

    private final I candidateId;
    private final int lastLogIndex;
    private final Term lastLogTerm;

    public RequestVoteRequest(Term term, I candidateId, int lastLogIndex, Optional<Term> lastLogTerm) {
        super(term, candidateId);
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm.orElse(null);
    }

    public I getCandidateId() {
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
                "candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                "} " + super.toString();
    }
}
