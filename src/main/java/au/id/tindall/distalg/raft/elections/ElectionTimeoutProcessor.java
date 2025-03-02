package au.id.tindall.distalg.raft.elections;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.processors.Processor;
import au.id.tindall.distalg.raft.processors.RaftProcessorGroup;

public class ElectionTimeoutProcessor<I> implements Processor<RaftProcessorGroup> {

    private final Server<I> server;

    public ElectionTimeoutProcessor(Server<I> server) {
        this.server = server;
    }

    @Override
    public ProcessResult process() {
        if (server.timeoutNowIfDue()) {
            return ProcessResult.BUSY;
        }
        return ProcessResult.IDLE;
    }

    @Override
    public RaftProcessorGroup getGroup() {
        return RaftProcessorGroup.REPLICATION;
    }
}
