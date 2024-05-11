package au.id.tindall.distalg.raft.rpc.clustermembership;

import java.io.Serializable;

public class RemoveServerRequest<I extends Serializable> implements ServerAdminRequest<RemoveServerResponse> {

    private final I oldServer;

    public RemoveServerRequest(I oldServer) {
        this.oldServer = oldServer;
    }

    public I getOldServer() {
        return oldServer;
    }
}
