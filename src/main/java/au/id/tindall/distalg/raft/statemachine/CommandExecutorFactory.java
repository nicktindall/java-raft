package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.snapshotting.Snapshotter;

import java.io.Serializable;

public class CommandExecutorFactory {

    public <ID extends Serializable> CommandExecutor<ID> createCommandExecutor(StateMachine stateMachine, ClientSessionStore clientSessionStore, Snapshotter<ID> snapshotter) {
        return new CommandExecutor<>(stateMachine, clientSessionStore, snapshotter);
    }
}
