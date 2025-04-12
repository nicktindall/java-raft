package au.id.tindall.distalg.raft.rpc.client;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Arrays;

public class ClientRequestResponse<I> implements ClientResponseMessage<I>, Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("ClientRequestResponse", ClientRequestResponse.class);

    private final ClientRequestStatus status;
    private final byte[] response;
    private final I leaderHint;

    public ClientRequestResponse(ClientRequestStatus status, byte[] response, I leaderHint) {
        this.status = status;
        this.response = response != null ? Arrays.copyOf(response, response.length) : null;
        this.leaderHint = leaderHint;
    }

    public ClientRequestResponse(StreamingInput streamingInput) {
        this(streamingInput.readEnum(ClientRequestStatus.class),
                streamingInput.readNullable(StreamingInput::readBytes),
                streamingInput.readNullable(StreamingInput::readIdentifier));
    }

    public ClientRequestStatus getStatus() {
        return status;
    }

    public byte[] getResponse() {
        return Arrays.copyOf(response, response.length);
    }

    @Override
    public I getLeaderHint() {
        return leaderHint;
    }

    @Override
    public boolean isFromLeader() {
        return status != ClientRequestStatus.NOT_LEADER;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeEnum(status);
        streamingOutput.writeNullable(response, StreamingOutput::writeBytes);
        streamingOutput.writeNullable(leaderHint, StreamingOutput::writeIdentifier);
    }
}
