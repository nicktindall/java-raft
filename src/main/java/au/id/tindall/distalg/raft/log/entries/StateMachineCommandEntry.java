package au.id.tindall.distalg.raft.log.entries;

import java.util.Arrays;

import au.id.tindall.distalg.raft.log.Term;

public class StateMachineCommandEntry extends LogEntry {

    private final byte[] command;

    public StateMachineCommandEntry(Term term, byte[] command) {
        super(term);
        this.command = Arrays.copyOf(command, command.length);
    }

    public byte[] getCommand() {
        return Arrays.copyOf(command, command.length);
    }

    @Override
    public String toString() {
        return "StateMachineCommandEntry{" +
                "command=" + Arrays.toString(command) +
                "} " + super.toString();
    }
}
