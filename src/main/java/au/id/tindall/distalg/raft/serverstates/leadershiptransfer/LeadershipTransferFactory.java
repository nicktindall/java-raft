package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class LeadershipTransferFactory<I extends Serializable> {

    private final Cluster<I> cluster;
    private final PersistentState<I> persistentState;

    public LeadershipTransferFactory(Cluster<I> cluster, PersistentState<I> persistentState) {
        this.cluster = cluster;
        this.persistentState = persistentState;
    }

    public LeadershipTransfer<I> create(ReplicationManager<I> replicationManager) {
        return new LeadershipTransfer<>(cluster, persistentState, replicationManager);
    }
}
