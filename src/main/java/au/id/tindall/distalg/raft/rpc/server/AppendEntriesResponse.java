package au.id.tindall.distalg.raft.rpc.server;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Optional;

public class AppendEntriesResponse<I> extends RpcMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AppendEntriesResponse", AppendEntriesResponse.class);

    private final boolean success;
    private final Integer appendedIndex;

    public AppendEntriesResponse(Term term, I source, boolean success, Optional<Integer> appendedIndex) {
        super(term, source);
        this.success = success;
        this.appendedIndex = appendedIndex.orElse(null);
    }

    @SuppressWarnings("unused")
    public AppendEntriesResponse(StreamingInput streamingInput) {
        super(streamingInput);
        this.success = streamingInput.readBoolean();
        this.appendedIndex = streamingInput.readNullable(StreamingInput::readInteger);
    }

    public boolean isSuccess() {
        return success;
    }

    public Optional<Integer> getAppendedIndex() {
        return Optional.ofNullable(appendedIndex);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeBoolean(success);
        streamingOutput.writeNullable(appendedIndex, StreamingOutput::writeInteger);
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "success=" + success +
                ", appendedIndex=" + appendedIndex +
                "} " + super.toString();
    }
}
