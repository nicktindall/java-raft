package au.id.tindall.distalg.raft.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientSessionStoreTest {

    private static final int MAX_SESSIONS = 2;
    private ClientSessionStore clientSessionStore;

    @BeforeEach
    void setUp() {
        clientSessionStore = new ClientSessionStore(MAX_SESSIONS);
    }

    @Test
    public void shouldStoreSessions() {
        clientSessionStore.createSession(1, 1);
        assertThat(clientSessionStore.hasSession(1)).isTrue();
    }

    @Test
    public void shouldThrowWhenClientIdAlreadyPresent() {
        clientSessionStore.createSession(1, 1);
        assertThatCode(() -> clientSessionStore.createSession(2, 1))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldIndicateWhenClientHasNoSession() {
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
    void recordInteraction_WillPreventSessionBeingExpired() {
        clientSessionStore.createSession(1, 1);
        clientSessionStore.createSession(2, 2);
        clientSessionStore.recordInteraction(1, 3);
        clientSessionStore.createSession(4, 3);

        assertThat(clientSessionStore.hasSession(1)).isTrue();
        assertThat(clientSessionStore.hasSession(2)).isFalse();
        assertThat(clientSessionStore.hasSession(3)).isTrue();
    }
}
