package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.state.Snapshot;

public interface StateMachine {

    byte[] apply(int index, byte[] command);

    byte[] createSnapshot();

    void installSnapshot(Snapshot snapshot);
}
