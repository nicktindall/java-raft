package au.id.tindall.distalg.raft.rpc.client;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Arrays;

public class ClientRequestRequest<I> implements ClientRequestMessage<ClientRequestResponse<I>>, Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("ClientRequestRequest", ClientRequestRequest.class);

    private final int clientId;
    private final int sequenceNumber;
    private final int lastResponseReceived;
    private final byte[] command;

    public ClientRequestRequest(int clientId, int sequenceNumber, int lastResponseReceived, byte[] command) {
        this.clientId = clientId;
        this.sequenceNumber = sequenceNumber;
        this.lastResponseReceived = lastResponseReceived;
        this.command = Arrays.copyOf(command, command.length);
    }

    @SuppressWarnings("unused")
    public ClientRequestRequest(StreamingInput streamingInput) {
        this.clientId = streamingInput.readIdentifier();
        this.sequenceNumber = streamingInput.readIdentifier();
        this.lastResponseReceived = streamingInput.readIdentifier();
        this.command = streamingInput.readBytes();
    }

    public int getClientId() {
        return clientId;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public int getLastResponseReceived() {
        return lastResponseReceived;
    }

    public byte[] getCommand() {
        return Arrays.copyOf(command, command.length);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeInteger(clientId);
        streamingOutput.writeInteger(sequenceNumber);
        streamingOutput.writeInteger(lastResponseReceived);
        streamingOutput.writeBytes(command);
    }
}
