package au.id.tindall.distalg.raft.comms;

import java.io.Serializable;

public interface ClusterFactory<I extends Serializable> {

    Cluster<I> createCluster(I nodeId);
}
