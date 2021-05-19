package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class LeadershipTransferFactory<ID extends Serializable> {

    private final Cluster<ID> cluster;
    private final PersistentState<ID> persistentState;

    public LeadershipTransferFactory(Cluster<ID> cluster, PersistentState<ID> persistentState) {
        this.cluster = cluster;
        this.persistentState = persistentState;
    }

    public LeadershipTransfer<ID> create(ReplicationManager<ID> replicationManager) {
        return new LeadershipTransfer<>(cluster, persistentState, replicationManager);
    }
}
