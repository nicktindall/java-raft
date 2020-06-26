package au.id.tindall.distalg.raft.threading;

import java.util.concurrent.ThreadFactory;

import static java.lang.String.format;

public class NamedThreadFactory implements ThreadFactory {

    private final ThreadGroup threadGroup;
    private int sequenceNumber = 0;

    public NamedThreadFactory(String threadGroupName) {
        this.threadGroup = new ThreadGroup(threadGroupName);
    }

    @Override
    public Thread newThread(Runnable runnable) {
        return new Thread(threadGroup, runnable, format("%s-%d", threadGroup.getName(), sequenceNumber++));
    }
}
