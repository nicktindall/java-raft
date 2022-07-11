package au.id.tindall.distalg.raft.statemachine;

public interface StateMachine {

    byte[] apply(byte[] command);

    byte[] createSnapshot();

    void installSnapshot(byte[] snapshot);
}
