package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;

public class CommandExecutorFactory {

    public CommandExecutor createCommandExecutor(StateMachine stateMachine, ClientSessionStore clientSessionStore) {
        return new CommandExecutor(stateMachine, clientSessionStore);
    }
}
