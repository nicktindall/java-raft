package au.id.tindall.distalg.raft.processors;

public interface Processor<G extends Enum<G>> {

    enum ProcessResult {
        BUSY,
        IDLE,
        FINISHED
    }

    default void beforeFirst() {
    }

    default void afterLast() {
    }

    ProcessResult process();

    G getGroup();

    default String getName() {
        return getClass().getSimpleName();
    }
}
