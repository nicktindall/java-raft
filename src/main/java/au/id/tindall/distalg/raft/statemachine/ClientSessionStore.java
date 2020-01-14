package au.id.tindall.distalg.raft.statemachine;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClientSessionStore {

    private static final Comparator<ClientSession> LAST_INTERACTION_COMPARATOR =
            Comparator.comparing(ClientSession::getLastInteractionSequence);
    private final int maxSessions;
    private final Map<Integer, ClientSession> activeSessions;
    private final EntryCommittedEventHandler clientRegistrationHandler;
    private final List<ClientSessionCreatedHandler> clientSessionCreatedHandlers;

    public ClientSessionStore(int maxSessions) {
        this.maxSessions = maxSessions;
        this.activeSessions = new HashMap<>();
        this.clientSessionCreatedHandlers = new ArrayList<>();
        this.clientRegistrationHandler = this::handleClientRegistrations;
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
        notifyListeners(registrationIndex, clientId);
    }

    private void notifyListeners(int logIndex, int clientId) {
        this.clientSessionCreatedHandlers.forEach(handler -> handler.clientSessionCreated(logIndex, clientId));
    }

    public void recordInteraction(int clientId, int index) {
        Optional.ofNullable(activeSessions.get(clientId))
                .ifPresent(session -> session.setLastInteractionSequence(index));
    }

    private void expireLeastRecentlyUsedSessionIfFull() {
        if (activeSessions.size() >= maxSessions) {
            ClientSession leastRecentlyUsedClientSession = activeSessions.values().stream()
                    .min(LAST_INTERACTION_COMPARATOR)
                    .orElseThrow(() -> new IllegalStateException("No active sessions is more than maximum?!"));
            activeSessions.remove(leastRecentlyUsedClientSession.getClientId());
        }
    }

    public void startListeningForClientRegistrations(Log log) {
        log.addEntryCommittedEventHandler(clientRegistrationHandler);
    }

    public void stopListeningForClientRegistrations(Log log) {
        log.removeEntryCommittedEventHandler(clientRegistrationHandler);
    }

    public void addClientSessionCreatedHandler(ClientSessionCreatedHandler clientSessionCreatedHandler) {
        this.clientSessionCreatedHandlers.add(clientSessionCreatedHandler);
    }

    public void removeClientSessionCreatedHandler(ClientSessionCreatedHandler clientSessionCreatedHandler) {
        this.clientSessionCreatedHandlers.remove(clientSessionCreatedHandler);
    }

    private void handleClientRegistrations(int index, LogEntry logEntry) {
        if (logEntry instanceof ClientRegistrationEntry) {
            createSession(index, ((ClientRegistrationEntry) logEntry).getClientId());
        }
    }
}
