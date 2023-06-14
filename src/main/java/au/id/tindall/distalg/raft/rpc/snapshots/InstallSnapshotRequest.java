package au.id.tindall.distalg.raft.rpc.snapshots;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.rpc.server.UnicastMessage;

import java.io.Serializable;

public class InstallSnapshotRequest<I extends Serializable> extends UnicastMessage<I> {

    private final I leaderId;
    private final int lastIndex;
    private final Term lastTerm;
    private final ConfigurationEntry lastConfig;
    private final int offset;
    private final int snapshotOffset;
    private final byte[] data;
    private final boolean done;

    public InstallSnapshotRequest(Term term, I leaderId, I destination,
                                  int lastIndex, Term lastTerm, ConfigurationEntry lastConfig,
                                  int snapshotOffset, int offset, byte[] data, boolean done) {
        super(term, leaderId, destination);
        this.leaderId = leaderId;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.lastConfig = lastConfig;
        this.snapshotOffset = snapshotOffset;
        this.offset = offset;
        this.data = data;
        this.done = done;
    }

    public I getLeaderId() {
        return leaderId;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public Term getLastTerm() {
        return lastTerm;
    }

    public ConfigurationEntry getLastConfig() {
        return lastConfig;
    }

    public int getOffset() {
        return offset;
    }

    public int getSnapshotOffset() {
        return snapshotOffset;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDone() {
        return done;
    }

    @Override
    public String toString() {
        return "InstallSnapshotRequest{" +
                "leaderId=" + leaderId +
                ", lastIndex=" + lastIndex +
                ", lastTerm=" + lastTerm +
                ", lastConfig=" + lastConfig +
                ", snapshotOffset=" + snapshotOffset +
                ", offset=" + offset +
                ", data.length=" + data.length +
                ", done=" + done +
                '}';
    }
}
