package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class RequestVoteResponse<I extends Serializable> extends UnicastMessage<I> {

    private final boolean voteGranted;

    public RequestVoteResponse(Term term, I source, I destination, boolean voteGranted) {
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
