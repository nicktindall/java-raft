package au.id.tindall.distalg.raft.statemachine;

public interface ClientSessionCreatedHandler {

    void clientSessionCreated(int logIndex, int clientId);
}
