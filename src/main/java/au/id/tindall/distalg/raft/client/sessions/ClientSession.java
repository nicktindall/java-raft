package au.id.tindall.distalg.raft.client.sessions;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;

public class ClientSession {

    private final int clientId;
    private int lastInteractionLogIndex;
    private final LinkedList<AppliedCommand> appliedCommands;

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
        appliedCommands.add(new AppliedCommand(sequenceNumber, result));
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

    static class AppliedCommand {

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
