package au.id.tindall.distalg.raft.replication;

public class SynchronousReplicationScheduler implements ReplicationScheduler {

    private Runnable sendAppendEntriesRequest;

    @Override
    public void setSendAppendEntriesRequest(Runnable sendAppendEntriesRequest) {
        this.sendAppendEntriesRequest = sendAppendEntriesRequest;
    }

    @Override
    public void start() {
        // Do nothing
    }

    @Override
    public void stop() {
        // Do nothing
    }

    @Override
    public void replicate() {
        sendAppendEntriesRequest.run();
    }
}
