package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class AddServerResponse<I> implements ClientResponseMessage<I>, Streamable {
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AddServerResponse", AddServerResponse.class);

    @SuppressWarnings("rawtypes")
    private static final AddServerResponse OK = new AddServerResponse(AddServerResponse.Status.OK);
    @SuppressWarnings("rawtypes")
    private static final AddServerResponse NOT_LEADER = new AddServerResponse(Status.NOT_LEADER);
    @SuppressWarnings("rawtypes")
    private static final AddServerResponse TIMEOUT = new AddServerResponse(Status.TIMEOUT);

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    @SuppressWarnings("unchecked")
    public static <I> AddServerResponse<I> getOK() {
        return OK;
    }

    @SuppressWarnings("unchecked")
    public static <I> AddServerResponse<I> getNotLeader() {
        return NOT_LEADER;
    }

    @SuppressWarnings("unchecked")
    public static <I> AddServerResponse<I> getTimeout() {
        return TIMEOUT;
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
    public boolean isFromLeader() {
        return status != Status.NOT_LEADER;
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
