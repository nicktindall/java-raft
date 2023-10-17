package au.id.tindall.distalg.raft.processors;

import java.io.Closeable;

public interface ProcessorDriver<G extends Enum<G>> extends Closeable {

    void run(ProcessorGroup<G> processorGroup);

    @Override
    default void close() {
    }
}
