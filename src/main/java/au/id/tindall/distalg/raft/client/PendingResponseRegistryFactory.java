package au.id.tindall.distalg.raft.client;

import au.id.tindall.distalg.raft.statemachine.ClientSessionStore;

public class PendingResponseRegistryFactory {

    public PendingResponseRegistry createPendingResponseRegistry(ClientSessionStore clientSessionStore) {
        return new PendingResponseRegistry(clientSessionStore);
    }
}