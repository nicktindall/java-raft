package au.id.tindall.distalg.raft.client;

import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.NOT_LEADER;
import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import au.id.tindall.distalg.raft.log.EntryCommittedEventHandler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ClientRegistryTest {

    private static final int CLIENT_ID = 123;
    private static final int OTHER_CLIENT_ID = 321;
    private ClientRegistry<Long> clientRegistry;

    @Mock
    private Log log;

    @BeforeEach
    public void setUp() {
        clientRegistry = new ClientRegistry<>();
    }

    @Nested
    class EntryCommitListener {

        @Test
        public void startListeningForEntryCommits_WillRegisterEventHandlerWithLog() {
            clientRegistry.startListeningForCommitEvents(log);
            verify(log).addEntryCommittedEventHandler(any(EntryCommittedEventHandler.class));
        }

        @Test
        public void stopListeningForEntryCommits_WillRemoveEventHandlerWithLog() {
            clientRegistry.stopListeningForCommitEvents(log);
            verify(log).removeEntryCommittedEventHandler(any(EntryCommittedEventHandler.class));
        }

        @Test
        public void startAndStopListeningForEntryCommits_WillPassSameHandlerToLog() {
            clientRegistry.startListeningForCommitEvents(log);
            clientRegistry.stopListeningForCommitEvents(log);
            ArgumentCaptor<EntryCommittedEventHandler> startedHandler = forClass(EntryCommittedEventHandler.class);
            ArgumentCaptor<EntryCommittedEventHandler> stoppedHandler = forClass(EntryCommittedEventHandler.class);
            verify(log).addEntryCommittedEventHandler(startedHandler.capture());
            verify(log).removeEntryCommittedEventHandler(stoppedHandler.capture());
            assertThat(startedHandler.getValue()).isSameAs(stoppedHandler.getValue());
        }
    }

    @Test
    public void responseFuture_WillResolve_WhenClientRegistrationEntryCommits() throws ExecutionException, InterruptedException {
        CompletableFuture<RegisterClientResponse<Long>> responseFuture = clientRegistry.createResponseFuture(CLIENT_ID);
        assertThat(responseFuture).isNotCompleted();
        clientRegistry.respondToClientRegistrationCommits(456, new ClientRegistrationEntry(new Term(5), CLIENT_ID));
        assertThat(responseFuture.get()).isEqualToComparingFieldByFieldRecursively(new RegisterClientResponse<>(OK, CLIENT_ID, null));
    }

    @Test
    public void failAnyOutstandingRegistrations_WillFailAllOutstandingRegistrations() throws ExecutionException, InterruptedException {
        CompletableFuture<RegisterClientResponse<Long>> clientFuture = clientRegistry.createResponseFuture(CLIENT_ID);
        CompletableFuture<RegisterClientResponse<Long>> otherClientFuture = clientRegistry.createResponseFuture(OTHER_CLIENT_ID);
        assertThat(clientFuture).isNotCompleted();
        assertThat(otherClientFuture).isNotCompleted();
        clientRegistry.failAnyOutstandingRegistrations();
        assertThat(clientFuture.get()).isEqualToComparingFieldByFieldRecursively(new RegisterClientResponse<>(NOT_LEADER, null, null));
        assertThat(otherClientFuture.get()).isEqualToComparingFieldByFieldRecursively(new RegisterClientResponse<>(NOT_LEADER, null, null));
    }
}