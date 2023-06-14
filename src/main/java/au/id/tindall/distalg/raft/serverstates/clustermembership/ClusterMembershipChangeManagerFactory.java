package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.time.Instant;

public class ClusterMembershipChangeManagerFactory<I extends Serializable> {

    private final Log log;
    private final PersistentState<I> persistentState;
    private final Configuration<I> configuration;

    public ClusterMembershipChangeManagerFactory(Log log, PersistentState<I> persistentState, Configuration<I> configuration) {
        this.log = log;
        this.persistentState = persistentState;
        this.configuration = configuration;
    }

    public ClusterMembershipChangeManager<I> createChangeManager(ReplicationManager<I> replicationManager) {
        return new ClusterMembershipChangeManager<>(new ClusterMembershipChangeFactory<>(log, configuration, persistentState, replicationManager, Instant::now));
    }
}
