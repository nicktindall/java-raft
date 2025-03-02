package au.id.tindall.distalg.raft.log.entries;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Arrays;

public class StateMachineCommandEntry extends LogEntry {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("StateMachineCommandEntry", StateMachineCommandEntry.class);

    private final int clientId;
    private final int lastResponseReceived;
    private final int clientSequenceNumber;
    private final byte[] command;

    public StateMachineCommandEntry(Term term, int clientId, int lastResponseReceived, int clientSequenceNumber, byte[] command) {
        super(term);
        this.clientId = clientId;
        this.lastResponseReceived = lastResponseReceived;
        this.clientSequenceNumber = clientSequenceNumber;
        this.command = Arrays.copyOf(command, command.length);
    }

    public StateMachineCommandEntry(StreamingInput streamingInput) {
        super(streamingInput);
        this.clientId = streamingInput.readInteger();
        this.lastResponseReceived = streamingInput.readInteger();
        this.clientSequenceNumber = streamingInput.readInteger();
        this.command = streamingInput.readBytes();
    }

    public int getClientId() {
        return clientId;
    }

    public int lastResponseReceived() {
        return lastResponseReceived;
    }

    public int getClientSequenceNumber() {
        return clientSequenceNumber;
    }

    public byte[] getCommand() {
        return Arrays.copyOf(command, command.length);
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeInteger(clientId);
        streamingOutput.writeInteger(lastResponseReceived);
        streamingOutput.writeInteger(clientSequenceNumber);
        streamingOutput.writeBytes(command);
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
