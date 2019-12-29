package au.id.tindall.distalg.raft.statemachine;

public class ClientSessionStoreFactory {

    public ClientSessionStore create(int maxSessions) {
        return new ClientSessionStore(maxSessions);
    }
}
