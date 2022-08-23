package au.id.tindall.distalg.raft.client.sessions;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.statemachine.CommandAppliedEventHandler;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class ClientSessionStoreTest {

    private static final int MAX_SESSIONS = 2;
    private static final byte[] RESULT = "result".getBytes();
    private ClientSessionStore clientSessionStore;

    @Mock
    private Log log;
    @Mock
    private ClientSessionCreatedHandler clientSessionCreatedHandler;
    @Mock
    private CommandExecutor commandExecutor;

    @BeforeEach
    void setUp() {
        clientSessionStore = new ClientSessionStore(MAX_SESSIONS);
    }

    @Test
    void shouldStoreSessions() {
        clientSessionStore.createSession(1, 1);
        assertThat(clientSessionStore.hasSession(1)).isTrue();
    }

    @Test
    void shouldThrowWhenClientIdAlreadyPresent() {
        clientSessionStore.createSession(1, 1);
        assertThatCode(() -> clientSessionStore.createSession(2, 1))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldIndicateWhenClientHasNoSession() {
        assertThat(clientSessionStore.hasSession(1)).isFalse();
    }

    @Test
    void shouldExpireLeastRecentlyUsedSession_WhenFull() {
        clientSessionStore.createSession(1, 1);
        clientSessionStore.createSession(2, 2);
        clientSessionStore.createSession(3, 3);

        assertThat(clientSessionStore.hasSession(1)).isFalse();
        assertThat(clientSessionStore.hasSession(2)).isTrue();
        assertThat(clientSessionStore.hasSession(3)).isTrue();
    }

    @Test
    void recordAppliedCommand_WillPreventSessionBeingExpired() {
        clientSessionStore.createSession(1, 1);
        clientSessionStore.createSession(2, 2);
        clientSessionStore.recordAppliedCommand(3, 1, -1, 0, "result".getBytes());
        clientSessionStore.createSession(4, 3);

        assertThat(clientSessionStore.hasSession(1)).isTrue();
        assertThat(clientSessionStore.hasSession(2)).isFalse();
        assertThat(clientSessionStore.hasSession(3)).isTrue();
    }

    @Test
    void recordAppliedCommand_WillTruncateCommandsToIndex() {
        clientSessionStore.createSession(10, 1);
        clientSessionStore.recordAppliedCommand(11, 1, -1, 0, RESULT);
        clientSessionStore.recordAppliedCommand(12, 1, -1, 1, RESULT);
        clientSessionStore.recordAppliedCommand(13, 1, 1, 2, RESULT);
        assertThat(clientSessionStore.getCommandResult(1, 0)).isEmpty();
        assertThat(clientSessionStore.getCommandResult(1, 1)).isEmpty();
        assertThat(clientSessionStore.getCommandResult(1, 2)).contains(RESULT);
    }

    @Test
    void shouldStartListeningForClientRegistrations() {
        clientSessionStore.startListeningForClientRegistrations(log);

        var eventHandler = ArgumentCaptor.forClass(EntryCommittedEventHandler.class);
        verify(log).addEntryCommittedEventHandler(eventHandler.capture());

        eventHandler.getValue().entryCommitted(100, new ClientRegistrationEntry(new Term(3), 100));

        assertThat(clientSessionStore.hasSession(100)).isTrue();
    }

    @Test
    void shouldStopListeningForClientRegistrations() {
        clientSessionStore.startListeningForClientRegistrations(log);

        var eventHandler = ArgumentCaptor.forClass(EntryCommittedEventHandler.class);
        verify(log).addEntryCommittedEventHandler(eventHandler.capture());

        clientSessionStore.stopListeningForClientRegistrations(log);
        verify(log).removeEntryCommittedEventHandler(eventHandler.getValue());
    }

    @Test
    void shouldStartListeningForAppliedCommands() {
        final int clientId = 9876;
        clientSessionStore.createSession(1, clientId);
        clientSessionStore.startListeningForAppliedCommands(commandExecutor);

        ArgumentCaptor<CommandAppliedEventHandler> argumentCaptor
                = ArgumentCaptor.forClass(CommandAppliedEventHandler.class);
        verify(commandExecutor).addCommandAppliedEventHandler(argumentCaptor.capture());

        final CommandAppliedEventHandler handler = argumentCaptor.getValue();
        handler.handleCommandApplied(13, clientId, -1, 1, RESULT);
        assertThat(clientSessionStore.getCommandResult(clientId, 1)).contains(RESULT);
        handler.handleCommandApplied(14, clientId, 1, 2, RESULT);
        assertThat(clientSessionStore.getCommandResult(clientId, 1)).isEmpty();
    }

    @Test
    void shouldEmitClientSessionCreatedEvents_WhenClientSessionCreated() {
        clientSessionStore.addClientSessionCreatedHandler(clientSessionCreatedHandler);

        clientSessionStore.createSession(1, 1);
        verify(clientSessionCreatedHandler).clientSessionCreated(1, 1);
    }

    @Test
    void shouldNotNotifyListeners_AfterTheyHaveBeenRemoved() {
        clientSessionStore.addClientSessionCreatedHandler(clientSessionCreatedHandler);
        clientSessionStore.createSession(1, 1);
        clientSessionStore.removeClientSessionCreatedHandler(clientSessionCreatedHandler);
        clientSessionStore.createSession(2, 2);
        verify(clientSessionCreatedHandler).clientSessionCreated(1, 1);
        verifyNoMoreInteractions(clientSessionCreatedHandler);
    }

    @Nested
    class GetCommandResult {

        @Test
        void returnsNothing_WhenNoSuchClientExists() {
            assertThat(clientSessionStore.getCommandResult(1, 0)).isEmpty();
        }

        @Test
        void returnsNothing_WhenNoMatchingCommandExists() {
            clientSessionStore.createSession(10, 1);
            assertThat(clientSessionStore.getCommandResult(1, 0)).isEmpty();
        }

        @Test
        void returnsCommandResult_WhenAMatchingCommandWasFound() {
            clientSessionStore.createSession(10, 1);
            clientSessionStore.recordAppliedCommand(11, 1, -1, 0, RESULT);
            assertThat(clientSessionStore.getCommandResult(1, 0)).contains(RESULT);
        }
    }

    @Test
    void shouldSerializeAndDeserializeActiveSessions() {
        clientSessionStore.createSession(1, 1);
        clientSessionStore.createSession(1, 2);

        clientSessionStore.recordAppliedCommand(1, 1, -1, 1, RESULT);
        clientSessionStore.recordAppliedCommand(2, 2, -1, 1, RESULT);
        clientSessionStore.recordAppliedCommand(3, 2, -1, 2, RESULT);

        final ClientSessionStore otherSessionStore = new ClientSessionStore(MAX_SESSIONS);
        otherSessionStore.replaceSessions(clientSessionStore.serializeSessions());

        assertThat(otherSessionStore.getCommandResult(1, 1)).contains(RESULT);
        assertThat(otherSessionStore.getCommandResult(2, 1)).contains(RESULT);
        assertThat(otherSessionStore.getCommandResult(2, 2)).contains(RESULT);
    }
}
