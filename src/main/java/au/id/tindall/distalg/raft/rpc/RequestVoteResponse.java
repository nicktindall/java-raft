package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;

import au.id.tindall.distalg.raft.log.Term;

public class RequestVoteResponse implements Serializable {

    private final Term term;
    private final boolean voteGranted;

    public RequestVoteResponse(Term term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public Term getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
