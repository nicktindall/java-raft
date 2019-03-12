package au.id.tindall.distalg.raft.log.entries;

import au.id.tindall.distalg.raft.log.Term;

public class ClientRegistrationEntry extends LogEntry {

    private final int clientId;

    public ClientRegistrationEntry(Term term, int clientId) {
        super(term);
        this.clientId = clientId;
    }

    public int getClientId() {
        return clientId;
    }

    @Override
    public String toString() {
        return "ClientRegistrationEntry{" +
                "clientId=" + clientId +
                "} " + super.toString();
    }
}
