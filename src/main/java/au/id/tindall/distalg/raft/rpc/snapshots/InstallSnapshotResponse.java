package au.id.tindall.distalg.raft.rpc.snapshots;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class InstallSnapshotResponse<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("InstallSnapshotResponse", InstallSnapshotResponse.class);

    private final boolean success;
    private final int lastIndex;
    private final int offset;

    public InstallSnapshotResponse(Term term, I source, boolean success, int lastIndex, int offset) {
        super(term, source);
        this.success = success;
        this.lastIndex = lastIndex;
        this.offset = offset;
    }

    @SuppressWarnings("unused")
    public InstallSnapshotResponse(StreamingInput streamingInput) {
        super(streamingInput);
        this.success = streamingInput.readBoolean();
        this.lastIndex = streamingInput.readInteger();
        this.offset = streamingInput.readInteger();
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

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeBoolean(success);
        streamingOutput.writeInteger(lastIndex);
        streamingOutput.writeInteger(offset);
    }
}
