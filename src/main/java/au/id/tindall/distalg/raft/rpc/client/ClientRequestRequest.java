package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;
import java.util.Arrays;

public class ClientRequestRequest<I extends Serializable> implements ClientRequestMessage<ClientRequestResponse<I>> {

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
}
