package au.id.tindall.distalg.raft.comms;

import java.io.Serializable;

public interface ClusterFactory<ID extends Serializable> {

    Cluster<ID> createForNode(ID nodeId);
}
