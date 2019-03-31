package au.id.tindall.distalg.raft.rpc.server;

import java.io.Serializable;

import au.id.tindall.distalg.raft.log.Term;

public class RequestVoteResponse<ID extends Serializable> extends UnicastMessage<ID> {

    private final boolean voteGranted;

    public RequestVoteResponse(Term term, ID source, ID destination, boolean voteGranted) {
        super(term, source, destination);
        this.voteGranted = voteGranted;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "RequestVoteResponse{" +
                "voteGranted=" + voteGranted +
                "} " + super.toString();
    }
}
