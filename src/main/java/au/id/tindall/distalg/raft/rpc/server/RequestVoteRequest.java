package au.id.tindall.distalg.raft.rpc.server;

import java.io.Serializable;
import java.util.Optional;

import au.id.tindall.distalg.raft.log.Term;

public class RequestVoteRequest<ID extends Serializable> extends BroadcastMessage<ID> {

    private final ID candidateId;
    private final int lastLogIndex;
    private final Term lastLogTerm;

    public RequestVoteRequest(Term term, ID candidateId, int lastLogIndex, Optional<Term> lastLogTerm) {
        super(term, candidateId);
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm.orElse(null);
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
                "candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                "} " + super.toString();
    }
}
