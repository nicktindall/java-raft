package au.id.tindall.distalg.raft.client.sessions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;

public class ClientSession implements Serializable {

    private final int clientId;
    private int lastInteractionLogIndex;
    private final LinkedList<AppliedCommand> appliedCommands;
    private Integer lastSequenceNumber = null;

    public ClientSession(int clientId, int registrationIndex) {
        this.clientId = clientId;
        this.lastInteractionLogIndex = registrationIndex;
        this.appliedCommands = new LinkedList<>();
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
            if (command.getSequenceNumber() == clientSequenceNumber) {
                return Optional.of(command.getResult());
            }
            if (command.getSequenceNumber() < clientSequenceNumber) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public void truncateAppliedCommands(int toIndex) {
        while (!appliedCommands.isEmpty()) {
            final AppliedCommand appliedCommand = appliedCommands.peek();
            if (appliedCommand.getSequenceNumber() <= toIndex) {
                appliedCommands.removeFirst();
            } else {
                break;
            }
        }
    }

    static class AppliedCommand implements Serializable {

        private final int sequenceNumber;
        private final byte[] result;

        public AppliedCommand(int sequenceNumber, byte[] result) {
            this.sequenceNumber = sequenceNumber;
            this.result = result;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public byte[] getResult() {
            return result;
        }
    }
}
