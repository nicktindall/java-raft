package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.PendingClientRequestResponse;
import au.id.tindall.distalg.raft.client.PendingRegisterClientResponse;
import au.id.tindall.distalg.raft.client.PendingResponse;
import au.id.tindall.distalg.raft.client.PendingResponseRegistry;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.statemachine.ClientSessionStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LeaderTest {

    private static final long SERVER_ID = 111;
    private static final long OTHER_SERVER_ID = 112;
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final int NEXT_LOG_INDEX = 4;
    private static final int LAST_LOG_INDEX = 3;
    private static final int CLIENT_ID = 55;
    private static final byte[] COMMAND = "TheCommand".getBytes();

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private LogReplicatorFactory<Long> logReplicatorFactory;
    @Mock
    private PendingResponseRegistry pendingResponseRegistry;
    @Mock
    private Log log;
    @Mock
    private LogReplicator<Long> otherServerLogReplicator;
    @Mock
    private ServerStateFactory<Long> serverStateFactory;
    @Mock
    private ClientSessionStore clientSessionStore;

    private Leader<Long> leader;

    @BeforeEach
    public void setUp() {
        when(log.getNextLogIndex()).thenReturn(NEXT_LOG_INDEX);
        when(cluster.getOtherMemberIds()).thenReturn(Set.of(OTHER_SERVER_ID));
        when(logReplicatorFactory.createLogReplicator(cluster, OTHER_SERVER_ID, NEXT_LOG_INDEX)).thenReturn(otherServerLogReplicator);
        leader = new Leader<>(TERM_2, log, cluster, pendingResponseRegistry, logReplicatorFactory, serverStateFactory, clientSessionStore);
    }

    @Nested
    class Constructor {

        @Test
        public void willCreateLogReplicators() {
            verify(logReplicatorFactory).createLogReplicator(cluster, OTHER_SERVER_ID, NEXT_LOG_INDEX);
        }
    }

    @Test
    public void dispose_WillDisposeOfPendingResponseRegistry() {
        leader.dispose();
        verify(pendingResponseRegistry).dispose();
    }

    @Nested
    class HandleAppendEntriesResponse {

        @Test
        public void willIgnoreMessage_WhenItIsStale() {
            leader.handle(new AppendEntriesResponse<>(TERM_1, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
            verifyNoMoreInteractions(otherServerLogReplicator, log);
        }

        @Test
        public void willLogSuccessResponseWithReplicatorThenUpdateCommitIndex_WhenResultIsSuccess() {
            int otherServerMatchIndex = 3;
            when(otherServerLogReplicator.getMatchIndex()).thenReturn(otherServerMatchIndex);

            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
            InOrder inOrder = inOrder(otherServerLogReplicator, log);
            inOrder.verify(otherServerLogReplicator).logSuccessResponse(5);
            inOrder.verify(log).updateCommitIndex(List.of(otherServerMatchIndex));
        }

        @Test
        public void willSendAHeartbeat_WhenTheCommitIndexIsAdvanced() {
            int otherServerMatchIndex = 3;
            when(otherServerLogReplicator.getMatchIndex()).thenReturn(otherServerMatchIndex);
            when(log.updateCommitIndex(anyList())).thenReturn(Optional.of(5));

            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));

            verify(otherServerLogReplicator, times(1)).sendAppendEntriesRequest(TERM_2, log);
        }

        @Test
        public void willNotSendAHeartbeat_WhenTheCommitIndexIsNotAdvanced() {
            int otherServerMatchIndex = 3;
            when(otherServerLogReplicator.getMatchIndex()).thenReturn(otherServerMatchIndex);
            when(log.updateCommitIndex(anyList())).thenReturn(Optional.empty());

            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));

            verify(otherServerLogReplicator, never()).sendAppendEntriesRequest(TERM_2, log);
        }

        @Test
        public void willLogFailureResponseWithReplicator_WhenResultIsFail() {
            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
            verify(otherServerLogReplicator).logFailedResponse();
        }

        @Test
        public void willNotUpdateCommitIndex_WhenResultIsFail() {
            leader.handle(new AppendEntriesResponse<>(TERM_2, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
            verify(log, never()).updateCommitIndex(anyList());
        }
    }

    @Nested
    class HandleRegisterClientRequest {

        @BeforeEach
        void setUp() {
            when(log.getLastLogIndex()).thenReturn(LAST_LOG_INDEX);
            when(pendingResponseRegistry.registerOutstandingResponse(anyInt(), any(PendingResponse.class)))
                    .thenAnswer(invocation -> ((PendingResponse) invocation.getArgument(1)).getResponseFuture());
        }

        @Test
        @SuppressWarnings("unchecked")
        void willAppendClientRegistrationLogEntry() {
            leader.handle(new RegisterClientRequest<>(SERVER_ID));

            ArgumentCaptor<List<LogEntry>> captor = ArgumentCaptor.forClass(List.class);
            verify(log).appendEntries(eq(LAST_LOG_INDEX), captor.capture());
            assertThat(captor.getValue()).usingFieldByFieldElementComparator().isEqualTo(
                    singletonList(new ClientRegistrationEntry(TERM_2, NEXT_LOG_INDEX)));
        }

        @Test
        void willTriggerReplication() {
            leader.handle(new RegisterClientRequest<>(SERVER_ID));

            verify(otherServerLogReplicator).sendAppendEntriesRequest(TERM_2, log);
        }

        @Test
        @SuppressWarnings("unchecked")
        void willRegisterAnOutstandingResponseAndReturnAssociatedPromise() {
            CompletableFuture<RegisterClientResponse<Long>> result = leader.handle(new RegisterClientRequest<>(SERVER_ID));

            ArgumentCaptor<PendingRegisterClientResponse<?>> captor = ArgumentCaptor.forClass(PendingRegisterClientResponse.class);
            verify(pendingResponseRegistry).registerOutstandingResponse(eq(NEXT_LOG_INDEX), captor.capture());
            assertThat(result).isSameAs(captor.getValue().getResponseFuture());
        }
    }

    @Nested
    class HandleClientRequestRequest {

        public static final int SEQUENCE_NUMBER = 0;

        @Nested
        class WhenClientHasNoActiveSession {

            @BeforeEach
            void setUp() {
                when(clientSessionStore.hasSession(CLIENT_ID)).thenReturn(false);
            }

            @Test
            void willReturnSessionExpiredResponse() throws ExecutionException, InterruptedException {
                assertThat(leader.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, SEQUENCE_NUMBER, COMMAND)).get())
                        .isEqualToComparingFieldByFieldRecursively(new ClientRequestResponse<>(ClientRequestStatus.SESSION_EXPIRED,
                                null, null));
            }
        }

        @Nested
        class WhenClientHasAnActiveSession {

            @BeforeEach
            void setUp() {
                when(clientSessionStore.hasSession(CLIENT_ID)).thenReturn(true);
                when(log.getLastLogIndex()).thenReturn(LAST_LOG_INDEX);
                when(pendingResponseRegistry.registerOutstandingResponse(anyInt(), any(PendingResponse.class)))
                        .thenAnswer(invocation -> ((PendingResponse) invocation.getArgument(1)).getResponseFuture());
            }

            @Test
            @SuppressWarnings("unchecked")
            void willAppendClientRegistrationLogEntry() {
                leader.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, SEQUENCE_NUMBER, COMMAND));

                ArgumentCaptor<List<LogEntry>> captor = ArgumentCaptor.forClass(List.class);
                verify(log).appendEntries(eq(LAST_LOG_INDEX), captor.capture());
                assertThat(captor.getValue()).usingFieldByFieldElementComparator().isEqualTo(
                        singletonList(new StateMachineCommandEntry(TERM_2, CLIENT_ID, SEQUENCE_NUMBER, COMMAND)));
            }

            @Test
            void willTriggerReplication() {
                leader.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, SEQUENCE_NUMBER, COMMAND));

                verify(otherServerLogReplicator).sendAppendEntriesRequest(TERM_2, log);
            }

            @Test
            @SuppressWarnings("unchecked")
            void willRegisterAnOutstandingResponseAndReturnAssociatedPromise() {
                CompletableFuture<ClientRequestResponse<Long>> result = leader.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, SEQUENCE_NUMBER, COMMAND));

                ArgumentCaptor<PendingClientRequestResponse<?>> captor = ArgumentCaptor.forClass(PendingClientRequestResponse.class);
                verify(pendingResponseRegistry).registerOutstandingResponse(eq(NEXT_LOG_INDEX), captor.capture());
                assertThat(result).isSameAs(captor.getValue().getResponseFuture());
            }
        }
    }
}