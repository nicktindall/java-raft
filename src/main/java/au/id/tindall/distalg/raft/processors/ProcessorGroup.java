package au.id.tindall.distalg.raft.processors;

import java.io.Serializable;

public interface ProcessorGroup<G extends Enum<G>> {

    Processor.ProcessResult runSingleIteration();

    void finalise();

    void add(Processor<G> processor);

    Serializable getServerID();

    G getGroup();
}
