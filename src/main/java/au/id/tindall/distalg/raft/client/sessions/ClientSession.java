package au.id.tindall.distalg.raft.client.sessions;

import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.Streamable;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;

public class ClientSession implements Streamable {

    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("ClientSession", ClientSession.class);

    private final int clientId;
    private int lastInteractionLogIndex;
    private final LinkedList<AppliedCommand> appliedCommands;
    private Integer lastSequenceNumber = null;

    public ClientSession(int clientId, int registrationIndex) {
        this.clientId = clientId;
        this.lastInteractionLogIndex = registrationIndex;
        this.appliedCommands = new LinkedList<>();
    }

    @SuppressWarnings("unused")
    public ClientSession(StreamingInput streamingInput) {
        this(streamingInput.readInteger(), streamingInput.readInteger());
        streamingInput.readList(ignored -> this.appliedCommands,
                StreamingInput::readStreamable);
        lastSequenceNumber = streamingInput.readNullable(StreamingInput::readInteger);
    }

    public int getLastInteractionLogIndex() {
        return lastInteractionLogIndex;
    }

    public Integer getClientId() {
        return clientId;
    }

    public void recordAppliedCommand(int logIndex, int sequenceNumber, byte[] result) {
        lastInteractionLogIndex = logIndex;
        if (lastSequenceNumber == null || sequenceNumber > lastSequenceNumber) {
            appliedCommands.add(new AppliedCommand(sequenceNumber, result));
            lastSequenceNumber = sequenceNumber;
        }
    }

    public Optional<byte[]> getCommandResult(int clientSequenceNumber) {
        Iterator<AppliedCommand> t = appliedCommands.descendingIterator();
        while (t.hasNext()) {
            AppliedCommand command = t.next();
            if (command.sequenceNumber() == clientSequenceNumber) {
                return Optional.of(command.result());
            }
            if (command.sequenceNumber() < clientSequenceNumber) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public void truncateAppliedCommands(int toIndex) {
        while (!appliedCommands.isEmpty()) {
            final AppliedCommand appliedCommand = appliedCommands.peek();
            if (appliedCommand.sequenceNumber() <= toIndex) {
                appliedCommands.removeFirst();
            } else {
                break;
            }
        }
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        streamingOutput.writeInteger(clientId);
        streamingOutput.writeInteger(lastInteractionLogIndex);
        streamingOutput.writeList(appliedCommands, StreamingOutput::writeStreamable);
        streamingOutput.writeNullable(lastSequenceNumber, StreamingOutput::writeInteger);
    }

    @SuppressWarnings("java:S6218")
    public record AppliedCommand(int sequenceNumber, byte[] result) implements Streamable {

        static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("AppliedCommand", AppliedCommand.class);

        @SuppressWarnings("unused")
        public AppliedCommand(StreamingInput streamingInput) {
            this(streamingInput.readInteger(), streamingInput.readBytes());
        }

        @Override
        public void writeTo(StreamingOutput streamingOutput) {
            streamingOutput.writeInteger(sequenceNumber);
            streamingOutput.writeBytes(result);
        }

        @Override
        public MessageIdentifier getMessageIdentifier() {
            return MESSAGE_IDENTIFIER;
        }
    }
}
