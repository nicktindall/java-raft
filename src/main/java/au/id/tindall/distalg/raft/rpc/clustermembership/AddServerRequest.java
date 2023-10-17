package au.id.tindall.distalg.raft.rpc.clustermembership;

import java.io.Serializable;

public class AddServerRequest<I extends Serializable> implements ServerAdminRequest {

    private final I newServer;

    public AddServerRequest(I newServer) {
        this.newServer = newServer;
    }

    public I getNewServer() {
        return newServer;
    }
}
