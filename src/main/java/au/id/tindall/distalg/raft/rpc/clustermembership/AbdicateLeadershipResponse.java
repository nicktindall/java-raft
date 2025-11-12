package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class AbdicateLeadershipResponse<I> implements ClientResponseMessage<I> {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AbdicateLeadershipResponse", AbdicateLeadershipResponse.class);
    @SuppressWarnings("rawtypes")
    private static final AbdicateLeadershipResponse OK = new AbdicateLeadershipResponse(Status.OK);
    @SuppressWarnings("rawtypes")
    private static final AbdicateLeadershipResponse NOT_LEADER = new AbdicateLeadershipResponse(Status.NOT_LEADER);

    @SuppressWarnings("unchecked")
    public static <I> AbdicateLeadershipResponse<I> getOK() {
        return OK;
    }

    @SuppressWarnings("unchecked")
    public static <I> AbdicateLeadershipResponse<I> getNotLeader() {
        return NOT_LEADER;
    }

    public AbdicateLeadershipResponse(Status status) {
        this.status = status;
    }

    @SuppressWarnings("unused")
    public AbdicateLeadershipResponse(StreamingInput streamingInput) {
        this(streamingInput.readEnum(Status.class));
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeEnum(status);
    }

    public enum Status {
        OK,
        NOT_LEADER
    }

    private final Status status;

    @Override
    public boolean isFromLeader() {
        return status != Status.NOT_LEADER;
    }
}
