package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;

public abstract class ClientRequestMessage<ID extends Serializable> {

    private final ID destinationId;

    public ClientRequestMessage(ID destinationId) {
        this.destinationId = destinationId;
    }

    public ID getDestinationId() {
        return destinationId;
    }

    @Override
    public String toString() {
        return "ClientRequestMessage{" +
                "destinationId=" + destinationId +
                '}';
    }
}
