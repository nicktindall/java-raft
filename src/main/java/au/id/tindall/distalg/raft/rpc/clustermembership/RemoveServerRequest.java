package au.id.tindall.distalg.raft.rpc.clustermembership;

import java.io.Serializable;

public class RemoveServerRequest<ID extends Serializable> implements ClusterMembershipRequest {

    private final ID oldServer;

    public RemoveServerRequest(ID oldServer) {
        this.oldServer = oldServer;
    }

    public ID getOldServer() {
        return oldServer;
    }
}
