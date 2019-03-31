package au.id.tindall.distalg.raft.rpc.client;

import java.io.Serializable;

public class RegisterClientRequest<ID extends Serializable> extends ClientRequestMessage<ID> {

    public RegisterClientRequest(ID destinationId) {
        super(destinationId);
    }
}
