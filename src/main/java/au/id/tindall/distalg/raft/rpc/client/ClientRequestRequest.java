package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;
import java.util.Arrays;

public class ClientRequestRequest<ID extends Serializable> extends ClientRequestMessage<ID> {

    private final int clientId;
    private final int sequenceNumber;
    private final byte[] command;

    public ClientRequestRequest(ID destinationId, int clientId, int sequenceNumber, byte[] command) {
        super(destinationId);
        this.clientId = clientId;
        this.sequenceNumber = sequenceNumber;
        this.command = Arrays.copyOf(command, command.length);
    }

    public int getClientId() {
        return clientId;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public byte[] getCommand() {
        return Arrays.copyOf(command, command.length);
    }
}
