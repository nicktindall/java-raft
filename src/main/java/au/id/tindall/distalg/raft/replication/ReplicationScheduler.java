package au.id.tindall.distalg.raft.replication;

public interface ReplicationScheduler {

    void setSendAppendEntriesRequest(Runnable sendAppendEntriesRequest);

    void start();

    void stop();

    void replicate();
}
