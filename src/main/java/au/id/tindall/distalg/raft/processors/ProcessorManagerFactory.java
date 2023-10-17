package au.id.tindall.distalg.raft.processors;

import java.io.Serializable;

public interface ProcessorManagerFactory {

    ProcessorManager<RaftProcessorGroup> create(Serializable serverID);
}
