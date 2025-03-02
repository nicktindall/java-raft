package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class AddServerResponse implements ClientResponseMessage, Streamable {
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AddServerResponse", AddServerResponse.class);

    public static final AddServerResponse OK = new AddServerResponse(AddServerResponse.Status.OK);
    public static final AddServerResponse NOT_LEADER = new AddServerResponse(Status.NOT_LEADER);
    public static final AddServerResponse TIMEOUT = new AddServerResponse(Status.TIMEOUT);

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    public AddServerResponse(Status status) {
        this.status = status;
    }

    public AddServerResponse(StreamingInput streamingInput) {
        this.status = streamingInput.readEnum(Status.class);
    }

    private final Status status;

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

    @Override
    public String toString() {
        return "AddServerResponse{" +
                "status=" + status +
                '}';
    }
}
