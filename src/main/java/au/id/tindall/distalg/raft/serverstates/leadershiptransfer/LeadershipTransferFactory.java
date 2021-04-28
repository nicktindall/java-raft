package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;
import java.util.Map;

public class LeadershipTransferFactory<ID extends Serializable> {

    private final Cluster<ID> cluster;
    private final PersistentState<ID> persistentState;

    public LeadershipTransferFactory(Cluster<ID> cluster, PersistentState<ID> persistentState) {
        this.cluster = cluster;
        this.persistentState = persistentState;
    }

    public LeadershipTransfer<ID> create(Map<ID, LogReplicator<ID>> replicators) {
        return new LeadershipTransfer<>(cluster, persistentState, replicators);
    }
}
