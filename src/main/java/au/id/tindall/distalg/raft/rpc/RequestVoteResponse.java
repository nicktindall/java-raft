package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;

import au.id.tindall.distalg.raft.log.Term;

public class RequestVoteResponse<ID extends Serializable> extends UnicastMessage<ID> {

    private final Term term;
    private final boolean voteGranted;

    public RequestVoteResponse(ID source, ID destination, Term term, boolean voteGranted) {
        super(source, destination);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public Term getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "RequestVoteResponse{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                "} " + super.toString();
    }
}
