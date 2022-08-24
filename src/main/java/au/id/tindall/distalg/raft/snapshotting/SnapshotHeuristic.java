package au.id.tindall.distalg.raft.snapshotting;

import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

public interface SnapshotHeuristic {

    SnapshotHeuristic NEVER_SNAPSHOT = (log, stateMachine, currentSnapshot) -> false;

    boolean shouldCreateSnapshot(Log log, StateMachine stateMachine, Snapshot currentSnapshot);
}
