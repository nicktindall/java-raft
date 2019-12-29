package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.statemachine.ClientSessionStore;
import au.id.tindall.distalg.raft.statemachine.ClientSessionStoreFactory;

import java.io.Serializable;

public class ServerFactory<ID extends Serializable> {

    private final ClusterFactory<ID> clusterFactory;
    private final LogFactory logFactory;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final ClientSessionStoreFactory clientSessionStoreFactory;
    private int maxClientSessions;

    public ServerFactory(ClusterFactory<ID> clusterFactory, LogFactory logFactory, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                         LogReplicatorFactory<ID> logReplicatorFactory, ClientSessionStoreFactory clientSessionStoreFactory, int maxClientSessions) {
        this.clusterFactory = clusterFactory;
        this.logFactory = logFactory;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.clientSessionStoreFactory = clientSessionStoreFactory;
        this.maxClientSessions = maxClientSessions;
    }

    public Server<ID> create(ID id) {
        ClientSessionStore clientSessionStore = clientSessionStoreFactory.create(maxClientSessions);
        Log log = logFactory.createLog();
        clientSessionStore.startListeningForClientRegistrations(log);
        return new Server<>(id, new ServerStateFactory<>(id, log, clusterFactory.createForNode(id), pendingResponseRegistryFactory, logReplicatorFactory, clientSessionStore));
    }
}
