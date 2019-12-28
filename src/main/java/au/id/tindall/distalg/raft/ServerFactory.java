package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;

import java.io.Serializable;

public class ServerFactory<ID extends Serializable> {

    private final ClusterFactory<ID> clusterFactory;
    private final LogFactory logFactory;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;

    public ServerFactory(ClusterFactory<ID> clusterFactory, LogFactory logFactory, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                         LogReplicatorFactory<ID> logReplicatorFactory) {
        this.clusterFactory = clusterFactory;
        this.logFactory = logFactory;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.logReplicatorFactory = logReplicatorFactory;
    }

    public Server<ID> create(ID id) {
        return new Server<>(id, new ServerStateFactory<>(id, logFactory.createLog(), clusterFactory.createForNode(id), pendingResponseRegistryFactory, logReplicatorFactory));
    }
}
