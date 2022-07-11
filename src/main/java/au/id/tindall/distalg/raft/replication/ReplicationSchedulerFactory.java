package au.id.tindall.distalg.raft.replication;

import java.io.Serializable;

public interface ReplicationSchedulerFactory<ID extends Serializable> {

    ReplicationScheduler create(ID serverId);
}
