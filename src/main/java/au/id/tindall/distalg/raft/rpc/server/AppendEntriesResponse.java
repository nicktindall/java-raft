package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;

import java.io.Serializable;
import java.util.Optional;

public class AppendEntriesResponse<I extends Serializable> extends RpcMessage<I> {

    private final boolean success;
    private final Integer appendedIndex;

    public AppendEntriesResponse(Term term, I source, boolean success, Optional<Integer> appendedIndex) {
        super(term, source);
        this.success = success;
        this.appendedIndex = appendedIndex.orElse(null);
    }

    public boolean isSuccess() {
        return success;
    }

    public Optional<Integer> getAppendedIndex() {
        return Optional.ofNullable(appendedIndex);
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "success=" + success +
                ", appendedIndex=" + appendedIndex +
                "} " + super.toString();
    }
}
