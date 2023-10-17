package au.id.tindall.distalg.raft.replication;

public class SynchronousReplicationScheduler implements ReplicationScheduler {

    @Override
    public void start() {
        // Do nothing
    }

    @Override
    public void stop() {
        // Do nothing
    }

    @Override
    public boolean replicationIsDue() {
        return false;
    }

    @Override
    public void replicated() {
        // Do nothing
    }
}
