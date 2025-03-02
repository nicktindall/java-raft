package au.id.tindall.distalg.raft.rpc.snapshots;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ConfigurationEntry;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class InstallSnapshotRequest<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("InstallSnapshotRequest", InstallSnapshotRequest.class);

    private final I leaderId;
    private final int lastIndex;
    private final Term lastTerm;
    private final ConfigurationEntry lastConfig;
    private final int offset;
    private final int snapshotOffset;
    private final byte[] data;
    private final boolean done;

    public InstallSnapshotRequest(Term term, I leaderId,
                                  int lastIndex, Term lastTerm, ConfigurationEntry lastConfig,
                                  int snapshotOffset, int offset, byte[] data, boolean done) {
        super(term, leaderId);
        this.leaderId = leaderId;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.lastConfig = lastConfig;
        this.snapshotOffset = snapshotOffset;
        this.offset = offset;
        this.data = data;
        this.done = done;
    }

    @SuppressWarnings("unused")
    public InstallSnapshotRequest(StreamingInput streamingInput) {
        super(streamingInput);
        this.leaderId = streamingInput.readIdentifier();
        this.lastIndex = streamingInput.readInteger();
        this.lastTerm = streamingInput.readStreamable();
        this.lastConfig = streamingInput.readStreamable();
        this.offset = streamingInput.readInteger();
        this.snapshotOffset = streamingInput.readInteger();
        this.data = streamingInput.readBytes();
        this.done = streamingInput.readBoolean();
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
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeIdentifier(leaderId);
        streamingOutput.writeInteger(lastIndex);
        streamingOutput.writeStreamable(lastTerm);
        streamingOutput.writeStreamable(lastConfig);
        streamingOutput.writeIdentifier(offset);
        streamingOutput.writeInteger(snapshotOffset);
        streamingOutput.writeBytes(data);
        streamingOutput.writeBoolean(done);
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
