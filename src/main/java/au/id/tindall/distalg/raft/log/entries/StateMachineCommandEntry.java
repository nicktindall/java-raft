package au.id.tindall.distalg.raft.log.entries;

import java.util.Arrays;

import au.id.tindall.distalg.raft.log.Term;

public class StateMachineCommandEntry extends LogEntry {

    private final int clientId;
    private final int clientSequenceNumber;
    private final byte[] command;

    public StateMachineCommandEntry(Term term, int clientId, int clientSequenceNumber, byte[] command) {
        super(term);
        this.clientId = clientId;
        this.clientSequenceNumber = clientSequenceNumber;
        this.command = Arrays.copyOf(command, command.length);
    }

    public int getClientId() {
        return clientId;
    }

    public int getClientSequenceNumber() {
        return clientSequenceNumber;
    }

    public byte[] getCommand() {
        return Arrays.copyOf(command, command.length);
    }

    @Override
    public String toString() {
        return "StateMachineCommandEntry{" +
                "clientId=" + clientId +
                ", clientSequenceNumber=" + clientSequenceNumber +
                ", command=" + Arrays.toString(command) +
                '}';
    }
}
