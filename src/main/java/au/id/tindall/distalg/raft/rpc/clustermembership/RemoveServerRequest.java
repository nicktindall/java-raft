package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class RemoveServerRequest<I> implements ClientRequestMessage<RemoveServerResponse>, Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("RemoveServerRequest", RemoveServerRequest.class);

    private final I oldServer;

    public RemoveServerRequest(I oldServer) {
        this.oldServer = oldServer;
    }

    @SuppressWarnings({"unused", "unchecked"})
    public RemoveServerRequest(StreamingInput streamingInput) {
        this((I) streamingInput.readIdentifier());
    }

    public I getOldServer() {
        return oldServer;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeIdentifier(oldServer);
    }
}
