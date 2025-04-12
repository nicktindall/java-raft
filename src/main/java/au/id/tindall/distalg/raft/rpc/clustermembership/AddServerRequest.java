package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class AddServerRequest<I> implements ClientRequestMessage<I, AddServerResponse<I>>, Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AddServerRequest", AddServerRequest.class);

    private final I newServer;

    public AddServerRequest(I newServer) {
        this.newServer = newServer;
    }

    @SuppressWarnings({"unchecked", "unused"})
    public AddServerRequest(StreamingInput streamingInput) {
        this((I) streamingInput.readIdentifier());
    }

    public I getNewServer() {
        return newServer;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeIdentifier(newServer);
    }
}
