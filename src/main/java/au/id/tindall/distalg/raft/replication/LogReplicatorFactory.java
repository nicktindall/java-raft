package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

import au.id.tindall.distalg.raft.comms.Cluster;

public class LogReplicatorFactory<ID extends Serializable> {

    public LogReplicator<ID> createLogReplicator(Cluster<ID> cluster, ID followerId, int nextIndex) {
        return new LogReplicator<>(cluster, followerId, nextIndex);
    }
}
