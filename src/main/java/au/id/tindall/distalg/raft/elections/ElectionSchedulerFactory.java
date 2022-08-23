package au.id.tindall.distalg.raft.elections;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

public class ElectionSchedulerFactory {

    private final long minimumElectionTimeoutMilliseconds;
    private final long maximumElectionTimeoutMilliseconds;
    private final ScheduledExecutorService scheduledExecutorService;

    public ElectionSchedulerFactory(ScheduledExecutorService scheduledExecutorService, long minimumElectionTimeoutMilliseconds, long maximumElectionTimeoutMilliseconds) {
        this.minimumElectionTimeoutMilliseconds = minimumElectionTimeoutMilliseconds;
        this.maximumElectionTimeoutMilliseconds = maximumElectionTimeoutMilliseconds;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public ElectionScheduler createElectionScheduler() {
        return new ElectionScheduler(new ElectionTimeoutGenerator(new Random(), minimumElectionTimeoutMilliseconds, maximumElectionTimeoutMilliseconds), scheduledExecutorService);
    }
}
