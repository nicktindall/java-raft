package au.id.tindall.distalg.raft.replication;

import java.util.function.Supplier;

public interface ReplicationScheduler {

    void setSendAppendEntriesRequest(Supplier<Boolean> sendAppendEntriesRequest);

    void start();

    void stop();

    void replicate();
}
