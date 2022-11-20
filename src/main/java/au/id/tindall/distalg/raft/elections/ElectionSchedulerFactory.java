package au.id.tindall.distalg.raft.elections;

import java.io.Serializable;
import java.time.Instant;
import java.util.Random;

public class ElectionSchedulerFactory<ID extends Serializable> {

    private final long minimumElectionTimeoutMilliseconds;
    private final long maximumElectionTimeoutMilliseconds;

    public ElectionSchedulerFactory(long minimumElectionTimeoutMilliseconds, long maximumElectionTimeoutMilliseconds) {
        this.minimumElectionTimeoutMilliseconds = minimumElectionTimeoutMilliseconds;
        this.maximumElectionTimeoutMilliseconds = maximumElectionTimeoutMilliseconds;
    }

    public ElectionScheduler<ID> createElectionScheduler(ID serverId) {
        return new ElectionScheduler<>(serverId, new ElectionTimeoutGenerator(new Random(), minimumElectionTimeoutMilliseconds, maximumElectionTimeoutMilliseconds), Instant::now);
    }
}
