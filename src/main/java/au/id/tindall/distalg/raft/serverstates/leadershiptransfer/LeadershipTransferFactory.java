package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;

public class LeadershipTransferFactory<I> {

    private final Cluster<I> cluster;
    private final PersistentState<I> persistentState;
    private final Configuration<I> configuration;

    public LeadershipTransferFactory(Cluster<I> cluster, PersistentState<I> persistentState, Configuration<I> configuration) {
        this.cluster = cluster;
        this.persistentState = persistentState;
        this.configuration = configuration;
    }

    public LeadershipTransfer<I> create(ReplicationManager<I> replicationManager) {
        return new LeadershipTransfer<>(cluster, persistentState, replicationManager, configuration);
    }
}
