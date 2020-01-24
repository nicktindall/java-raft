package au.id.tindall.distalg.raft.client.sessions;

public interface ClientSessionCreatedHandler {

    void clientSessionCreated(int logIndex, int clientId);
}
