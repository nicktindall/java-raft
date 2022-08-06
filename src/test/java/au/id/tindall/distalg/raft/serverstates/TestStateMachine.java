package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.util.ArrayList;
import java.util.List;

public class TestStateMachine implements StateMachine {

    private final List<byte[]> appliedCommands;

    public TestStateMachine() {
        this.appliedCommands = new ArrayList<>();
    }

    @Override
    public byte[] apply(int index, byte[] command) {
        this.appliedCommands.add(command);
        return new byte[]{(byte) appliedCommands.size()};
    }

    @Override
    public byte[] createSnapshot() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void installSnapshot(Snapshot snapshot) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public List<byte[]> getAppliedCommands() {
        return appliedCommands;
    }
}
