package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.util.ThreadUtil;

public enum SleepStrategies {
    ;

    private static final SleepStrategy THREAD_SLEEP = () -> ThreadUtil.pauseMillis(1);

    public static SleepStrategy threadSleep() {
        return THREAD_SLEEP;
    }

    public static SleepStrategy yielding() {
        return Thread::yield;
    }
}
