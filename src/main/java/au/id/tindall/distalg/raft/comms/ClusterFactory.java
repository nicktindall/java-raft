package au.id.tindall.distalg.raft.comms;

public interface ClusterFactory<I> {

    Cluster<I> createCluster(I nodeId);
}
