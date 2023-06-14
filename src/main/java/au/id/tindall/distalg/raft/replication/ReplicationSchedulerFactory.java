package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public interface ReplicationSchedulerFactory<I extends Serializable> {

    ReplicationScheduler create(I serverId);
}
