package au.id.tindall.distalg.raft.driver;

import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;

public class HeartbeatSchedulerFactory<ID extends Serializable> {

    private final long delayBetweenHeartbeatsInMilliseconds;

    public HeartbeatSchedulerFactory(long delayBetweenHeartbeatsInMilliseconds) {
        this.delayBetweenHeartbeatsInMilliseconds = delayBetweenHeartbeatsInMilliseconds;
    }

    public HeartbeatScheduler<ID> createHeartbeatScheduler(ScheduledExecutorService scheduledExecutorService) {
        return new HeartbeatScheduler<ID>(delayBetweenHeartbeatsInMilliseconds, scheduledExecutorService);
    }
}
