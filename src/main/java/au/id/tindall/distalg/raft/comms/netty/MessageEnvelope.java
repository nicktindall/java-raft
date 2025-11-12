package au.id.tindall.distalg.raft.comms.netty;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public record MessageEnvelope(long correlationId, Streamable message) implements Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("MessageEnvelope", MessageEnvelope.class);

    @SuppressWarnings("unused")
    public MessageEnvelope(StreamingInput streamingInput) {
        this(streamingInput.readLong(), streamingInput.readStreamable());
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeLong(correlationId);
        streamingOutput.writeStreamable(message);
    }
}
