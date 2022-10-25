package au.id.tindall.distalg.raft.threading;

import java.util.concurrent.ThreadFactory;

import static java.lang.String.format;

public class NamedThreadFactory implements ThreadFactory {

    private final ThreadGroup threadGroup;
    private final boolean sequenced;
    private int sequenceNumber = 0;

    public static NamedThreadFactory forSingleThread(String threadName) {
        return new NamedThreadFactory(threadName, false);
    }

    public static NamedThreadFactory forThreadGroup(String threadGroupName) {
        return new NamedThreadFactory(threadGroupName, true);
    }

    private NamedThreadFactory(String threadGroupName, boolean sequenced) {
        this.threadGroup = new ThreadGroup(threadGroupName);
        this.sequenced = sequenced;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        return new Thread(threadGroup, runnable, sequenced ? format("%s-%d", threadGroup.getName(), sequenceNumber++) : threadGroup.getName());
    }
}
