package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;

import au.id.tindall.distalg.raft.log.Term;

public class RequestVoteResponse<ID extends Serializable> implements Serializable {

    private final ID source;
    private final Term term;
    private final boolean voteGranted;

    public RequestVoteResponse(ID source, Term term, boolean voteGranted) {
        this.source = source;
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public ID getSource() {
        return source;
    }

    public Term getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
