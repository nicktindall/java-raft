package au.id.tindall.distalg.raft.processors;

public interface ProcessorController {

    void stop();

    void stopAndWait();

    ProcessorState state();
}
