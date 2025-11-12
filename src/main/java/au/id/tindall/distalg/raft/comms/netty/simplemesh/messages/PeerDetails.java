package au.id.tindall.distalg.raft.comms.netty.simplemesh.messages;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public record PeerDetails(int id, long nodeTimestamp, long detailsTimestamp,
                          List<String> socketAddresses) implements Streamable {
    private static final Comparator<PeerDetails> VERSION_COMPARATOR = Comparator.comparing(PeerDetails::nodeTimestamp)
            .thenComparing(PeerDetails::detailsTimestamp);
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("SimpleMesh.PeerDetails", PeerDetails.class);

    @SuppressWarnings("unused")
    public PeerDetails(StreamingInput streamingInput) {
        this(streamingInput.readInteger(),
                streamingInput.readLong(),
                streamingInput.readLong(),
                streamingInput.readList(ArrayList::new, StreamingInput::readString));
    }


    public boolean isNewerThan(PeerDetails other) {
        return VERSION_COMPARATOR.compare(this, other) > 0;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeInteger(id);
        streamingOutput.writeLong(nodeTimestamp);
        streamingOutput.writeLong(detailsTimestamp);
        streamingOutput.writeList(socketAddresses, StreamingOutput::writeString);
    }
}
