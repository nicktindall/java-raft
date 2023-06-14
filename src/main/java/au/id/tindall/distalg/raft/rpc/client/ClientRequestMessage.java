package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;

public abstract class ClientRequestMessage<I extends Serializable> {

    private final I destinationId;

    protected ClientRequestMessage(I destinationId) {
        this.destinationId = destinationId;
    }

    public I getDestinationId() {
        return destinationId;
    }

    @Override
    public String toString() {
        return "ClientRequestMessage{" +
                "destinationId=" + destinationId +
                '}';
    }
}
