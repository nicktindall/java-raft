package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;


import au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionStateMachine;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import io.netty.channel.ChannelHandlerContext;

public record PayloadMessage(Streamable payload) implements Streamable, StateChangeInvoker {
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("SimpleMesh.Payload", PayloadMessage.class);

    @SuppressWarnings("unused")
    public PayloadMessage(StreamingInput streamingInput) {
        this(streamingInput.<Streamable>readStreamable());
    }

    @Override
    public ConnectionStateMachine.ConnectionState apply(ChannelHandlerContext ctx, ConnectionStateMachine.ConnectionState currentState) {
        return currentState.onPayloadMessage(this, ctx);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeStreamable(payload);
    }
}