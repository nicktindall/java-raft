package au.id.tindall.distalg.raft.replication;

import java.util.function.Supplier;

public class SynchronousReplicationScheduler implements ReplicationScheduler {

    private Supplier<Boolean> sendAppendEntriesRequest;

    @Override
    public void setSendAppendEntriesRequest(Supplier<Boolean> sendAppendEntriesRequest) {
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
        sendAppendEntriesRequest.get();
    }
}
