package au.id.tindall.distalg.raft.elections;

import java.io.Serializable;
import java.time.Instant;
import java.util.Random;

public class ElectionSchedulerFactory<I extends Serializable> {

    private final long minimumElectionTimeoutMilliseconds;
    private final long maximumElectionTimeoutMilliseconds;

    public ElectionSchedulerFactory(long minimumElectionTimeoutMilliseconds, long maximumElectionTimeoutMilliseconds) {
        this.minimumElectionTimeoutMilliseconds = minimumElectionTimeoutMilliseconds;
        this.maximumElectionTimeoutMilliseconds = maximumElectionTimeoutMilliseconds;
    }

    @SuppressWarnings("unused")
    public ElectionScheduler createElectionScheduler(I serverId) {
        return new ElectionScheduler(minimumElectionTimeoutMilliseconds, new ElectionTimeoutGenerator(new Random(), minimumElectionTimeoutMilliseconds, maximumElectionTimeoutMilliseconds), Instant::now);
    }
}
