package au.id.tindall.distalg.raft.rpc.clustermembership;

import java.io.Serializable;

public class AddServerRequest<ID extends Serializable> extends ClusterMembershipRequest<ID> {

    private final ID newServer;

    public AddServerRequest(ID newServer) {
        this.newServer = newServer;
    }

    public ID getNewServer() {
        return newServer;
    }
}
