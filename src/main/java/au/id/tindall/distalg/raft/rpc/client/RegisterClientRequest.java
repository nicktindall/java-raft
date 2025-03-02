package au.id.tindall.distalg.raft.rpc.client;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class RegisterClientRequest<I> implements ClientRequestMessage<RegisterClientResponse<I>>, Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("RegisterClientRequest", RegisterClientRequest.class);

    public RegisterClientRequest() {
    }

    @SuppressWarnings("unused")
    public RegisterClientRequest(StreamingInput streamingInput) {
        // There are no contents
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        // There are no contents
    }
}
