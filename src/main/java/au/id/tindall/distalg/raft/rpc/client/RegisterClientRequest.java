package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;

public class RegisterClientRequest<I extends Serializable> extends ClientRequestMessage<I> {

    public RegisterClientRequest(I destinationId) {
        super(destinationId);
    }
}
