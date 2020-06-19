package au.id.tindall.distalg.raft.client.sessions;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
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
        clientSessionStore.recordAppliedCommand(3, 1, 0, "result".getBytes());
        clientSessionStore.createSession(4, 3);

        assertThat(clientSessionStore.hasSession(1)).isTrue();
        assertThat(clientSessionStore.hasSession(2)).isFalse();
        assertThat(clientSessionStore.hasSession(3)).isTrue();
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
            clientSessionStore.recordAppliedCommand(11, 1, 0, RESULT);
            assertThat(clientSessionStore.getCommandResult(1, 0)).contains(RESULT);
        }
    }
}
