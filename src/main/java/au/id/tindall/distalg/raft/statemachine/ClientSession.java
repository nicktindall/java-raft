package au.id.tindall.distalg.raft.statemachine;

public class ClientSession {

    private final int clientId;
    private int lastInteractionSequence;

    public ClientSession(int clientId, int registrationIndex) {
        this.clientId = clientId;
        this.lastInteractionSequence = registrationIndex;
    }

    public int getLastInteractionSequence() {
        return lastInteractionSequence;
    }

    public Integer getClientId() {
        return clientId;
    }

    public void setLastInteractionSequence(int lastInteractionSequence) {
        this.lastInteractionSequence = lastInteractionSequence;
    }
}
