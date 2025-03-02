package au.id.tindall.distalg.raft.processors;

public interface ProcessorGroup<G extends Enum<G>> {

    Processor.ProcessResult runSingleIteration();

    void finalise();

    void add(Processor<G> processor);

    Object getServerID();

    G getGroup();
}
