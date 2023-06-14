package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.state.PersistentState;

import java.io.Serializable;

public class SnapshotReplicatorFactory<I extends Serializable> {

    private final PersistentState<I> persistentState;
    private final Cluster<I> cluster;

    public SnapshotReplicatorFactory(PersistentState<I> persistentState, Cluster<I> cluster) {
        this.persistentState = persistentState;
        this.cluster = cluster;
    }

    public SnapshotReplicator<I> createSnapshotReplicator(ReplicationState<I> replicationState) {
        return new SnapshotReplicator<>(persistentState.getCurrentTerm(), cluster, persistentState, replicationState);
    }
}
