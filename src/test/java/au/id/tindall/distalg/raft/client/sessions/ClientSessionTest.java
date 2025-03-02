package au.id.tindall.distalg.raft.client.sessions;

import au.id.tindall.distalg.raft.serialisation.UnsupportedIDSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static au.id.tindall.distalg.raft.SerializationUtils.roundTripSerializeDeserialize;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class ClientSessionTest {

    private static final int REGISTRATION_INDEX = 456;
    private static final int CLIENT_ID = 123;
    private static final byte[] RESULT = "results".getBytes();
    private static final byte[] OTHER_RESULT = "results".getBytes();

    private ClientSession clientSession;

    @BeforeEach
    void setUp() {
        clientSession = new ClientSession(CLIENT_ID, REGISTRATION_INDEX);
    }

    @Nested
    class GetLastInteractionLogIndex {

        @Test
        void returnsRegistrationIndex_BeforeAnyCommandsAreApplied() {
            assertThat(clientSession.getLastInteractionLogIndex()).isEqualTo(REGISTRATION_INDEX);
        }

        @Test
        void returnsMostRecentAppliedCommandIndex_WhenCommandsHaveBeenApplied() {
            clientSession.recordAppliedCommand(REGISTRATION_INDEX + 1, 0, RESULT);
            clientSession.recordAppliedCommand(REGISTRATION_INDEX + 2, 1, RESULT);
            assertThat(clientSession.getLastInteractionLogIndex()).isEqualTo(REGISTRATION_INDEX + 2);
        }

        @Test
        void returnsMostRecentIndex_WhenDuplicateCommandsAreApplied() {
            clientSession.recordAppliedCommand(REGISTRATION_INDEX + 1, 0, RESULT);
            clientSession.recordAppliedCommand(REGISTRATION_INDEX + 2, 0, RESULT);
            assertThat(clientSession.getLastInteractionLogIndex()).isEqualTo(REGISTRATION_INDEX + 2);
        }
    }

    @Nested
    class GetCommandResponse {

        @Test
        void returnsEmpty_WhenNoCommandsHaveBeenApplied() {
            assertThat(clientSession.getCommandResult(0)).isEmpty();
        }

        @Test
        void returnsCommandResult_WhenCommandHasBeenApplied() {
            clientSession.recordAppliedCommand(123, 0, RESULT);
            assertThat(clientSession.getCommandResult(0)).contains(RESULT);
        }

        @Test
        void returnsFirstCommandResult_WhenCommandIsDuplicated() {
            clientSession.recordAppliedCommand(123, 0, RESULT);
            // This would never happen, it is just to illustrate that duplicate commands are recorded once
            clientSession.recordAppliedCommand(124, 0, OTHER_RESULT);
            assertThat(clientSession.getCommandResult(0)).contains(RESULT);
        }
    }

    @Test
    void testIsStreamable() {
        ClientSession session = new ClientSession(123, 456);
        session.recordAppliedCommand(7891, 1, "test-1".getBytes());
        session.recordAppliedCommand(7892, 2, "test-2".getBytes());
        session.recordAppliedCommand(7893, 3, "test-3".getBytes());
        assertThat(roundTripSerializeDeserialize(session, UnsupportedIDSerializer.INSTANCE)).usingRecursiveComparison()
                .isEqualTo(session);
    }
}