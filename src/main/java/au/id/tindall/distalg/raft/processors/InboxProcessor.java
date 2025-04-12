package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.comms.Cluster;

public class InboxProcessor<I> implements Processor<RaftProcessorGroup> {

    private final Server<I> server;
    private final Cluster<I> cluster;
    private final Runnable onStart;
    private final Runnable onStop;

    public InboxProcessor(Server<I> server, Cluster<I> cluster, Runnable onStart, Runnable onStop) {
        this.server = server;
        this.cluster = cluster;
        this.onStart = onStart;
        this.onStop = onStop;
    }

    @Override
    public void beforeFirst() {
        onStart.run();
    }

    @Override
    public void afterLast() {
        onStop.run();
    }

    @Override
    public ProcessResult process() {
        ProcessResult result = ProcessResult.IDLE;
        while (cluster.processNextMessage(server)) {
            result = ProcessResult.BUSY;
        }
        return result;
    }

    @Override
    public RaftProcessorGroup getGroup() {
        return RaftProcessorGroup.REPLICATION;
    }
}
