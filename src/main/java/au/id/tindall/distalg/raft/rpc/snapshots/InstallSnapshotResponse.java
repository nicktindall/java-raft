package au.id.tindall.distalg.raft.rpc.snapshots;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.UnicastMessage;

import java.io.Serializable;

public class InstallSnapshotResponse<I extends Serializable> extends UnicastMessage<I> {

    private final boolean success;
    private final int lastIndex;
    private final int offset;

    public InstallSnapshotResponse(Term term, I source, I destination, boolean success, int lastIndex, int offset) {
        super(term, source, destination);
        this.success = success;
        this.lastIndex = lastIndex;
        this.offset = offset;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public int getOffset() {
        return offset;
    }
}
