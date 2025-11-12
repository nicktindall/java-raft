package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionStateMachine;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Parser;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import io.netty.channel.ChannelHandlerContext;

public record BlockMessage() implements Streamable, StateChangeInvoker {
    public static final BlockMessage INSTANCE = new BlockMessage();
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("SimpleMesh.Block", BlockMessage.class);

    @Parser
    public static BlockMessage readFrom(StreamingInput ignored) {
        // There is only one
        return INSTANCE;
    }

    @Override
    public ConnectionStateMachine.ConnectionState apply(ChannelHandlerContext ctx, ConnectionStateMachine.ConnectionState currentState) {
        return currentState.onBlock(this, ctx);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        // No contents
    }
}
