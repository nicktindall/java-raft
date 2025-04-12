package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class RemoveServerResponse<I> implements ClientResponseMessage<I>, Streamable {
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("RemoveServerResponse", RemoveServerResponse.class);

    @SuppressWarnings("rawtypes")
    private static final RemoveServerResponse OK = new RemoveServerResponse(Status.OK);
    @SuppressWarnings("rawtypes")
    private static final RemoveServerResponse NOT_LEADER = new RemoveServerResponse(Status.NOT_LEADER);

    public enum Status {
        OK,
        NOT_LEADER,
        TIMEOUT
    }

    @SuppressWarnings("unchecked")
    public static <I> RemoveServerResponse<I> getOK() {
        return OK;
    }

    @SuppressWarnings("unchecked")
    public static <I> RemoveServerResponse<I> getNotLeader() {
        return NOT_LEADER;
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
}
