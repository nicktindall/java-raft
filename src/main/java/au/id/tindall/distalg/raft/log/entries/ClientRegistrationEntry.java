package au.id.tindall.distalg.raft.log.entries;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.serialisation.MessageIdentifier;
import au.id.tindall.distalg.raft.serialisation.StreamingInput;
import au.id.tindall.distalg.raft.serialisation.StreamingOutput;

public class ClientRegistrationEntry extends LogEntry {
    private static final MessageIdentifier MESSAGE_IDENTIFIER = MessageIdentifier.registerMessageIdentifier("ClientRegistrationEntry", ClientRegistrationEntry.class);

    private final int clientId;

    public ClientRegistrationEntry(Term term, int clientId) {
        super(term);
        this.clientId = clientId;
    }

    public ClientRegistrationEntry(StreamingInput streamingInput) {
        super(streamingInput);
        this.clientId = streamingInput.readInteger();
    }

    public int getClientId() {
        return clientId;
    }

    @Override
    public MessageIdentifier getMessageIdentifier() {
        return MESSAGE_IDENTIFIER;
    }

    @Override
    public void writeTo(StreamingOutput streamingOutput) {
        super.writeTo(streamingOutput);
        streamingOutput.writeInteger(clientId);
    }

    @Override
    public String toString() {
        return "ClientRegistrationEntry{" +
                "clientId=" + clientId +
                "} " + super.toString();
    }
}
