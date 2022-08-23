package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.snapshotting.Snapshotter;

public class CommandExecutorFactory {

    public CommandExecutor createCommandExecutor(StateMachine stateMachine, ClientSessionStore clientSessionStore, Snapshotter snapshotter) {
        return new CommandExecutor(stateMachine, clientSessionStore, snapshotter);
    }
}
