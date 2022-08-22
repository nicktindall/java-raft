package au.id.tindall.distalg.raft.client.sessions;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.state.Snapshot;
import au.id.tindall.distalg.raft.state.SnapshotInstalledListener;
import au.id.tindall.distalg.raft.statemachine.CommandAppliedEventHandler;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static au.id.tindall.distalg.raft.util.SerializationUtil.deserializeObject;
import static au.id.tindall.distalg.raft.util.SerializationUtil.serializeObject;

public class ClientSessionStore implements SnapshotInstalledListener {

    private static final Comparator<ClientSession> LAST_INTERACTION_COMPARATOR =
            Comparator.comparing(ClientSession::getLastInteractionLogIndex);
    private final int maxSessions;
    private final EntryCommittedEventHandler clientRegistrationHandler;
    private final List<ClientSessionCreatedHandler> clientSessionCreatedHandlers;
    private final CommandAppliedEventHandler commandAppliedEventHandler;
    private Map<Integer, ClientSession> activeSessions;

    public ClientSessionStore(int maxSessions) {
        this.maxSessions = maxSessions;
        this.activeSessions = new HashMap<>();
        this.clientSessionCreatedHandlers = new ArrayList<>();
        this.clientRegistrationHandler = this::handleClientRegistrations;
        this.commandAppliedEventHandler = this::recordAppliedCommand;
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
        notifySessionCreatedListeners(registrationIndex, clientId);
    }

    private void notifySessionCreatedListeners(int logIndex, int clientId) {
        this.clientSessionCreatedHandlers.forEach(handler -> handler.clientSessionCreated(logIndex, clientId));
    }

    public void recordAppliedCommand(int logIndex, int clientId, int lastResponseReceived, int sequenceNumber, byte[] result) {
        Optional.ofNullable(activeSessions.get(clientId))
                .ifPresent(session -> {
                    session.truncateAppliedCommands(lastResponseReceived);
                    session.recordAppliedCommand(logIndex, sequenceNumber, result);
                });
    }

    private void expireLeastRecentlyUsedSessionIfFull() {
        if (activeSessions.size() >= maxSessions) {
            ClientSession leastRecentlyUsedClientSession = activeSessions.values().stream()
                    .min(LAST_INTERACTION_COMPARATOR)
                    .orElseThrow(() -> new IllegalStateException("No active sessions is more than maximum?!"));
            activeSessions.remove(leastRecentlyUsedClientSession.getClientId());
        }
    }

    public void startListeningForAppliedCommands(CommandExecutor<?> commandExecutor) {
        commandExecutor.addCommandAppliedEventHandler(this.commandAppliedEventHandler);
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

    public Optional<byte[]> getCommandResult(int clientId, int clientSequenceNumber) {
        return Optional.ofNullable(activeSessions.get(clientId))
                .flatMap(session -> session.getCommandResult(clientSequenceNumber));
    }

    public byte[] serializeSessions() {
        return serializeObject(activeSessions);
    }

    @Override
    public void onSnapshotInstalled(Snapshot snapshot) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(snapshot.snapshotOffset());
        snapshot.readInto(byteBuffer, 0);
        replaceSessions(byteBuffer.array());
    }

    public void replaceSessions(byte[] sessions) {
        activeSessions = deserializeObject(sessions);
    }
}
