package au.id.tindall.distalg.raft.elections;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

public class ElectionSchedulerFactory<ID extends Serializable> {

    private final long minimumElectionTimeoutMilliseconds;
    private final long maximumElectionTimeoutMilliseconds;

    public ElectionSchedulerFactory(long minimumElectionTimeoutMilliseconds, long maximumElectionTimeoutMilliseconds) {
        this.minimumElectionTimeoutMilliseconds = minimumElectionTimeoutMilliseconds;
        this.maximumElectionTimeoutMilliseconds = maximumElectionTimeoutMilliseconds;
    }

    public ElectionScheduler<ID> createElectionScheduler(ScheduledExecutorService scheduledExecutorService) {
        return new ElectionScheduler<>(new ElectionTimeoutGenerator(new Random(), minimumElectionTimeoutMilliseconds, maximumElectionTimeoutMilliseconds), scheduledExecutorService);
    }
}
