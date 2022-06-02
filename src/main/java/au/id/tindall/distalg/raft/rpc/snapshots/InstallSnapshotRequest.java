package au.id.tindall.distalg.raft.rpc.snapshots;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.rpc.server.UnicastMessage;

import java.io.Serializable;

public class InstallSnapshotRequest<ID extends Serializable> extends UnicastMessage<ID> {

    private final ID leaderId;
    private final int lastIndex;
    private final Term lastTerm;
    private final ConfigurationEntry lastConfig;
    private final int offset;
    private final byte[] data;
    private final boolean done;

    public InstallSnapshotRequest(Term term, ID leaderId, ID destination,
                                  int lastIndex, Term lastTerm, ConfigurationEntry lastConfig,
                                  int offset, byte[] data, boolean done) {
        super(term, leaderId, destination);
        this.leaderId = leaderId;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.lastConfig = lastConfig;
        this.offset = offset;
        this.data = data;
        this.done = done;
    }

    public ID getLeaderId() {
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

    public byte[] getData() {
        return data;
    }

    public boolean isDone() {
        return done;
    }
}
