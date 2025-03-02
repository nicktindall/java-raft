package au.id.tindall.distalg.raft.replication;

public interface ReplicationSchedulerFactory<I> {

    ReplicationScheduler create(I serverId);
}
