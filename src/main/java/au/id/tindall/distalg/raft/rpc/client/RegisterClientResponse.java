package au.id.tindall.distalg.raft.rpc.client;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Optional;

public class RegisterClientResponse<I> implements ClientResponseMessage<I>, Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("RegisterClientResponse", RegisterClientResponse.class);

    private final RegisterClientStatus status;
    private final Integer clientId;
    private final I leaderHint;

    public RegisterClientResponse(RegisterClientStatus status, Integer clientId, I leaderHint) {
        this.status = status;
        this.clientId = clientId;
        this.leaderHint = leaderHint;
    }

    public RegisterClientResponse(StreamingInput streamingInput) {
        this.status = streamingInput.readEnum(RegisterClientStatus.class);
        this.clientId = streamingInput.readNullable(StreamingInput::readInteger);
        this.leaderHint = streamingInput.readNullable(StreamingInput::readIdentifier);
    }

    public RegisterClientStatus getStatus() {
        return status;
    }

    public Optional<Integer> getClientId() {
        return Optional.ofNullable(clientId);
    }

    @Override
    public I getLeaderHint() {
        return leaderHint;
    }

    @Override
    public boolean isFromLeader() {
        return status != RegisterClientStatus.NOT_LEADER;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeEnum(status);
        streamingOutput.writeNullable(clientId, StreamingOutput::writeInteger);
        streamingOutput.writeNullable(leaderHint, StreamingOutput::writeIdentifier);
    }

    @Override
    public String toString() {
        return "RegisterClientResponse{" +
                "status=" + status +
                ", clientId=" + clientId +
                ", leaderHint=" + leaderHint +
                "} " + super.toString();
    }
}
