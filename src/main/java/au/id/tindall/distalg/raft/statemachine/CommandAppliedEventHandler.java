package au.id.tindall.distalg.raft.statemachine;

public interface CommandAppliedEventHandler {

    void handleCommandApplied(int index, byte[] result);
}
