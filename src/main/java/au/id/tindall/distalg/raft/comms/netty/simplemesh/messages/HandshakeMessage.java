package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionStateMachine;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import io.netty.channel.ChannelHandlerContext;

public record HandshakeMessage(PeerDetails peerDetails) implements Streamable, StateChangeInvoker {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("SimpleMesh.Handshake", HandshakeMessage.class);

    @SuppressWarnings("unused")
    public HandshakeMessage(StreamingInput streamingInput) {
        this(streamingInput.<PeerDetails>readStreamable());
    }

    @Override
    public ConnectionStateMachine.ConnectionState apply(ChannelHandlerContext ctx, ConnectionStateMachine.ConnectionState currentState) {
        return currentState.onHandshake(this, ctx);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeStreamable(peerDetails);
    }
}