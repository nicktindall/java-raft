package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class SnapshotReplicatorFactory<ID extends Serializable> {

    private final PersistentState<ID> persistentState;
    private final Cluster<ID> cluster;

    public SnapshotReplicatorFactory(PersistentState<ID> persistentState, Cluster<ID> cluster) {
        this.persistentState = persistentState;
        this.cluster = cluster;
    }

    public SnapshotReplicator<ID> createSnapshotReplicator(ReplicationState<ID> replicationState) {
        return new SnapshotReplicator<>(persistentState.getCurrentTerm(), cluster, persistentState, replicationState);
    }
}
