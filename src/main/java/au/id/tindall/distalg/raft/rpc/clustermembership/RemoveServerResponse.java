package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class RemoveServerResponse implements ClientResponseMessage, Streamable {
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("RemoveServerResponse", RemoveServerResponse.class);

    public static final RemoveServerResponse OK = new RemoveServerResponse(Status.OK);
    public static final RemoveServerResponse NOT_LEADER = new RemoveServerResponse(Status.NOT_LEADER);

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    private final Status status;

    public RemoveServerResponse(Status status) {
        this.status = status;
    }

    public RemoveServerResponse(StreamingInput streamingInput) {
        this(streamingInput.readEnum(Status.class));
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeEnum(status);
    }
}
