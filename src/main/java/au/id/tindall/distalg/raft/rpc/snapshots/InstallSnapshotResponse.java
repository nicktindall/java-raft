package au.id.tindall.distalg.raft.rpc.snapshots;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;

import java.io.Serializable;

public class InstallSnapshotResponse<I extends Serializable> extends RpcMessage<I> {

    private final boolean success;
    private final int lastIndex;
    private final int offset;

    public InstallSnapshotResponse(Term term, I source, boolean success, int lastIndex, int offset) {
        super(term, source);
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
