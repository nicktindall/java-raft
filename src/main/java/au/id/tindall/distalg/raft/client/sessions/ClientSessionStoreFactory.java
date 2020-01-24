package au.id.tindall.distalg.raft.client.sessions;

public class ClientSessionStoreFactory {

    public ClientSessionStore create(int maxSessions) {
        return new ClientSessionStore(maxSessions);
    }
}
