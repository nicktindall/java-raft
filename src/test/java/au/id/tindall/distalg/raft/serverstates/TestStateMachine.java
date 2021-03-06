package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.statemachine.StateMachine;

import java.util.ArrayList;
import java.util.List;

public class TestStateMachine implements StateMachine {

    private final List<byte[]> appliedCommands;

    public TestStateMachine() {
        this.appliedCommands = new ArrayList<>();
    }

    @Override
    public byte[] apply(byte[] command) {
        this.appliedCommands.add(command);
        return new byte[]{(byte) appliedCommands.size()};
    }

    public List<byte[]> getAppliedCommands() {
        return appliedCommands;
    }
}
