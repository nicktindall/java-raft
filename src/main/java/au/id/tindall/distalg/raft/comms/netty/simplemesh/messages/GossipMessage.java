package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;

import au.id.tindall.distalg.raft.comms.netty.simplemesh.ConnectionStateMachine;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

public record GossipMessage(List<PeerDetails> peerDetails) implements Streamable, StateChangeInvoker {
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("SimpleMesh.GossipMessage", GossipMessage.class);

    @SuppressWarnings("unused")
    public GossipMessage(StreamingInput streamingInput) {
        this(streamingInput.readList(ArrayList::new, StreamingInput::readStreamable));
    }

    @Override
    public ConnectionStateMachine.ConnectionState apply(ChannelHandlerContext ctx, ConnectionStateMachine.ConnectionState currentState) {
        return currentState.onGossip(this, ctx);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeList(peerDetails, StreamingOutput::writeStreamable);
    }
}