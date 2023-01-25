package au.id.tindall.distalg.raft.elections;

import java.time.Instant;
import java.util.Random;

public class ElectionSchedulerFactory {

    private final long minimumElectionTimeoutMilliseconds;
    private final long maximumElectionTimeoutMilliseconds;

    public ElectionSchedulerFactory(long minimumElectionTimeoutMilliseconds, long maximumElectionTimeoutMilliseconds) {
        this.minimumElectionTimeoutMilliseconds = minimumElectionTimeoutMilliseconds;
        this.maximumElectionTimeoutMilliseconds = maximumElectionTimeoutMilliseconds;
    }

    public ElectionScheduler createElectionScheduler() {
        return new ElectionScheduler(new ElectionTimeoutGenerator(new Random(), minimumElectionTimeoutMilliseconds, maximumElectionTimeoutMilliseconds), Instant::now);
    }
}
