package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Parser;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class AbdicateLeadershipRequest<I> implements ClientRequestMessage<I, AbdicateLeadershipResponse<I>> {

    @SuppressWarnings("rawtypes")
    private static final AbdicateLeadershipRequest INSTANCE = new AbdicateLeadershipRequest();
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AbdicateLeadershipRequest", AbdicateLeadershipRequest.class);

    private AbdicateLeadershipRequest() {
    }

    @SuppressWarnings("unchecked")
    public static <I> AbdicateLeadershipRequest<I> instance() {
        return INSTANCE;
    }

    @Parser
    public static <I> AbdicateLeadershipRequest<I> parse(StreamingInput streamingInput) {
        return instance();
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        // No content
    }
}
