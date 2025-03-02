package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.replication.ReplicationManager;

public class ReplicationProcessor<I> implements Processor<RaftProcessorGroup> {

    private final ReplicationManager<I> replicationManager;

    public ReplicationProcessor(ReplicationManager<I> replicationManager) {
        this.replicationManager = replicationManager;
    }

    @Override
    public ProcessResult process() {
        boolean replicated = replicationManager.replicateIfDue();
        return replicated ? ProcessResult.BUSY : ProcessResult.IDLE;
    }

    @Override
    public RaftProcessorGroup getGroup() {
        return RaftProcessorGroup.REPLICATION;
    }
}
