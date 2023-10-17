package au.id.tindall.distalg.raft.replication;

public interface ReplicationScheduler {

    void start();

    void stop();

    boolean replicationIsDue();

    void replicated();
}
