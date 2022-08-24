package au.id.tindall.distalg.raft.snapshotting;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

public interface SnapshotterFactory {

    Snapshotter create(Log log, ClientSessionStore clientSessionStore, StateMachine stateMachine, PersistentState<?> persistentState, SnapshotHeuristic snapshotHeuristic);
}
