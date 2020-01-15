package au.id.tindall.distalg.raft.client;

import au.id.tindall.distalg.raft.statemachine.ClientSessionStore;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;

public class PendingResponseRegistryFactory {

    public PendingResponseRegistry createPendingResponseRegistry(ClientSessionStore clientSessionStore, CommandExecutor commandExecutor) {
        return new PendingResponseRegistry(clientSessionStore, commandExecutor);
    }
}