package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.time.Instant;

public class ClusterMembershipChangeManagerFactory<ID extends Serializable> {

    private final Log log;
    private final PersistentState<ID> persistentState;
    private final Configuration<ID> configuration;

    public ClusterMembershipChangeManagerFactory(Log log, PersistentState<ID> persistentState, Configuration<ID> configuration) {
        this.log = log;
        this.persistentState = persistentState;
        this.configuration = configuration;
    }

    public ClusterMembershipChangeManager<ID> createChangeManager(ReplicationManager<ID> replicationManager) {
        return new ClusterMembershipChangeManager<>(new ClusterMembershipChangeFactory<>(log, configuration, persistentState, replicationManager, Instant::now));
    }
}
