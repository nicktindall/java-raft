package au.id.tindall.distalg.raft.rpc;

import java.io.Serializable;
import java.util.Optional;

import au.id.tindall.distalg.raft.log.Term;

public class AppendEntriesResponse<ID extends Serializable> extends UnicastMessage<ID> {

    private final Term term;
    private final boolean success;
    private final Integer appendedIndex;

    public AppendEntriesResponse(ID source, ID destination, Term term, boolean success, Optional<Integer> appendedIndex) {
        super(source, destination);
        this.term = term;
        this.success = success;
        this.appendedIndex = appendedIndex.orElse(null);
    }

    public Term getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public Optional<Integer> getAppendedIndex() {
        return Optional.of(appendedIndex);
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                ", appendedIndex=" + appendedIndex +
                "} " + super.toString();
    }
}
