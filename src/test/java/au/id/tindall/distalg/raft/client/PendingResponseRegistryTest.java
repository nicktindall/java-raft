package au.id.tindall.distalg.raft.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PendingResponseRegistryTest {

    private static final int LOG_INDEX = 456;
    private static final int OTHER_LOG_INDEX = 789;
    private PendingResponseRegistry pendingResponseRegistry;

    @Mock
    private PendingResponse<ClientResponseMessage> pendingResponse;
    @Mock
    private PendingResponse<ClientResponseMessage> otherPendingResponse;
    @Mock
    private Log log;
    @Mock
    private LogEntry logEntry;

    @BeforeEach
    void setUp() {
        pendingResponseRegistry = new PendingResponseRegistry();
    }

    @Nested
    class EntryCommitListener {

        @Test
        void startListeningForEntryCommits_WillRegisterEventHandlerWithLog() {
            pendingResponseRegistry.startListeningForCommitEvents(log);
            verify(log).addEntryCommittedEventHandler(any(EntryCommittedEventHandler.class));
        }

        @Test
        void stopListeningForEntryCommits_WillRemoveEventHandlerWithLog() {
            pendingResponseRegistry.stopListeningForCommitEvents(log);
            verify(log).removeEntryCommittedEventHandler(any(EntryCommittedEventHandler.class));
        }

        @Test
        void startAndStopListeningForEntryCommits_WillPassSameHandlerToLog() {
            pendingResponseRegistry.startListeningForCommitEvents(log);
            pendingResponseRegistry.stopListeningForCommitEvents(log);
            ArgumentCaptor<EntryCommittedEventHandler> startedHandler = forClass(EntryCommittedEventHandler.class);
            ArgumentCaptor<EntryCommittedEventHandler> stoppedHandler = forClass(EntryCommittedEventHandler.class);
            verify(log).addEntryCommittedEventHandler(startedHandler.capture());
            verify(log).removeEntryCommittedEventHandler(stoppedHandler.capture());
            assertThat(startedHandler.getValue()).isSameAs(stoppedHandler.getValue());
        }
    }

    @Nested
    class HandleEntryCommitted {

        @Test
        void willCompleteResponse_WhenOneIsRegisteredAtTheCommittedIndex() {
            pendingResponseRegistry.registerOutstandingResponse(LOG_INDEX, pendingResponse);
            pendingResponseRegistry.handleEntryCommitted(LOG_INDEX, logEntry);

            verify(pendingResponse).completeSuccessfully(logEntry);
        }

        @Test
        void willDoNothing_WhenThereIsNoResponseRegistered() {
            pendingResponseRegistry.handleEntryCommitted(LOG_INDEX, logEntry);
        }
    }

    @Test
    void registerPendingResponse_WillReturnPendingResponseFuture() {
        CompletableFuture<ClientResponseMessage> future = new CompletableFuture<>();
        when(pendingResponse.getResponseFuture()).thenReturn(future);
        assertThat(pendingResponseRegistry.registerOutstandingResponse(LOG_INDEX, pendingResponse)).isSameAs(future);
    }

    @Test
    void failOutstandingResponses_WillFailAllOutstandingResponses() {
        pendingResponseRegistry.registerOutstandingResponse(LOG_INDEX, pendingResponse);
        pendingResponseRegistry.registerOutstandingResponse(OTHER_LOG_INDEX, otherPendingResponse);
        pendingResponseRegistry.failOutstandingResponses();
        verify(pendingResponse).generateFailedResponse();
        verify(otherPendingResponse).generateFailedResponse();
    }
}