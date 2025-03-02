package au.id.tindall.distalg.raft.processors;

public interface ProcessorManagerFactory {

    ProcessorManager<RaftProcessorGroup> create(Object serverID);
}
