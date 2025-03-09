package au.id.tindall.distalg.raft.rpc.clustermembership;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;

import java.io.Serializable;

public class AddServerRequest<I extends Serializable> implements ClientRequestMessage<AddServerResponse> {

    private final I newServer;

    public AddServerRequest(I newServer) {
        this.newServer = newServer;
    }

    public I getNewServer() {
        return newServer;
    }
}
