package au.id.tindall.distalg.raft.statemachine;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class ClientSessionStore {

    private static final Comparator<ClientSession> LAST_INTERACTION_COMPARATOR =
            Comparator.comparing(ClientSession::getLastInteractionSequence);
    private final int maxSessions;
    private final Map<Integer, ClientSession> activeSessions;

    public ClientSessionStore(int maxSessions) {
        this.maxSessions = maxSessions;
        this.activeSessions = new HashMap<>();
    }

    public boolean hasSession(int clientId) {
        return activeSessions.containsKey(clientId);
    }

    public void createSession(int registrationIndex, int clientId) {
        if (activeSessions.containsKey(clientId)) {
            throw new IllegalStateException("Attempted to create multiple sessions for client ID " + clientId);
        }
        expireLeastRecentlyUsedSessionIfFull();
        activeSessions.put(clientId, new ClientSession(clientId, registrationIndex));
    }

    private void expireLeastRecentlyUsedSessionIfFull() {
        if (activeSessions.size() >= maxSessions) {
            ClientSession leastRecentlyUsedClientSession = activeSessions.values().stream()
                    .min(LAST_INTERACTION_COMPARATOR)
                    .orElseThrow(() -> new IllegalStateException("No active sessions is more than maximum?!"));
            activeSessions.remove(leastRecentlyUsedClientSession.getClientId());
        }
    }
}
