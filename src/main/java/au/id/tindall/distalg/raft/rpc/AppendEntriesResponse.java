package au.id.tindall.distalg.raft.rpc;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;

public class AppendEntriesResponse<ID extends Serializable> extends UnicastMessage<ID> {

    private final Term term;
    private final boolean success;

    public AppendEntriesResponse(ID source, ID destination, Term term, boolean success) {
        super(source, destination);
        this.term = term;
        this.success = success;
    }

    public Term getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                "} " + super.toString();
    }
}
