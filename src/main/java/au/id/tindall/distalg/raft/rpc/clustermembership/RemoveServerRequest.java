package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;

import java.io.Serializable;

public class RemoveServerRequest<I extends Serializable> implements ClientRequestMessage<RemoveServerResponse> {

    private final I oldServer;

    public RemoveServerRequest(I oldServer) {
        this.oldServer = oldServer;
    }

    public I getOldServer() {
        return oldServer;
    }
}
