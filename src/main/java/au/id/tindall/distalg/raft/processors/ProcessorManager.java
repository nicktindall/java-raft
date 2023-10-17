package au.id.tindall.distalg.raft.processors;

import java.io.Closeable;

public interface ProcessorManager<G extends Enum<G>> extends Closeable {

    ProcessorController runProcessor(Processor<G> processor);
}
