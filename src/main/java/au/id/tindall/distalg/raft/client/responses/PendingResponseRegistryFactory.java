package au.id.tindall.distalg.raft.client.responses;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;

public class PendingResponseRegistryFactory {

    public PendingResponseRegistry createPendingResponseRegistry(ClientSessionStore clientSessionStore, CommandExecutor commandExecutor) {
        return new PendingResponseRegistry(clientSessionStore, commandExecutor);
    }
}