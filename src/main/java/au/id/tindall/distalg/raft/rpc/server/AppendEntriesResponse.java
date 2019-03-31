package au.id.tindall.distalg.raft.rpc.server;

import java.io.Serializable;
import java.util.Optional;

import au.id.tindall.distalg.raft.log.Term;

public class AppendEntriesResponse<ID extends Serializable> extends UnicastMessage<ID> {

    private final boolean success;
    private final Integer appendedIndex;

    public AppendEntriesResponse(Term term, ID source, ID destination, boolean success, Optional<Integer> appendedIndex) {
        super(term, source, destination);
        this.success = success;
        this.appendedIndex = appendedIndex.orElse(null);
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
                "success=" + success +
                ", appendedIndex=" + appendedIndex +
                "} " + super.toString();
    }
}
