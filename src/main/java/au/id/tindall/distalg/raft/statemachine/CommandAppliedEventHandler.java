package au.id.tindall.distalg.raft.statemachine;

public interface CommandAppliedEventHandler {

    void handleCommandApplied(int logIndex, int clientId, int lastResponseReceived, int sequenceNumber, byte[] result);
}
