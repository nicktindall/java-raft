package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingClientRequestResponse;
import au.id.tindall.distalg.raft.client.responses.PendingRegisterClientResponse;
import au.id.tindall.distalg.raft.client.responses.PendingResponse;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
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
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransfer;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderTest {

    private static final long SERVER_ID = 111;
    private static final long OTHER_SERVER_ID = 112;
    private static final Term PREVIOUS_TERM = new Term(1);
    private static final Term CURRENT_TERM = new Term(2);
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
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private LeadershipTransferFactory<Long> leadershipTransferFactory;
    @Mock
    private LeadershipTransfer<Long> leadershipTransfer;

    private Leader<Long> leader;

    @BeforeEach
    void setUp() {
        when(leadershipTransferFactory.create(any())).thenReturn(leadershipTransfer);
        when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);
        when(log.getNextLogIndex()).thenReturn(NEXT_LOG_INDEX);
        when(cluster.getOtherMemberIds()).thenReturn(Set.of(OTHER_SERVER_ID));
        when(logReplicatorFactory.createLogReplicator(log, CURRENT_TERM, cluster, OTHER_SERVER_ID, NEXT_LOG_INDEX)).thenReturn(otherServerLogReplicator);
        leader = new Leader<>(persistentState, log, cluster, pendingResponseRegistry, logReplicatorFactory, serverStateFactory, clientSessionStore, leadershipTransferFactory);
    }

    @Nested
    class Constructor {

        @Test
        void willCreateLogReplicators() {
            verify(logReplicatorFactory).createLogReplicator(log, CURRENT_TERM, cluster, OTHER_SERVER_ID, NEXT_LOG_INDEX);
        }
    }

    @Nested
    class OnEnterState {

        @BeforeEach
        void setUp() {
            leader.enterState();
        }

        @Test
        void willStartReplicators() {
            verify(otherServerLogReplicator).start();
        }

        @Test
        void willReplicateToEveryoneToClaimLeadership() {
            verify(otherServerLogReplicator).replicate();
        }
    }

    @Nested
    class OnLeaveState {

        @BeforeEach
        void setUp() {
            leader.leaveState();
        }

        @Test
        void willStopReplicators() {
            verify(otherServerLogReplicator).stop();
        }

        @Test
        void willDisposeOfPendingResponseRegistry() {
            verify(pendingResponseRegistry).dispose();
        }
    }

    @Nested
    class HandleAppendEntriesResponse {

        @Nested
        class WhenMessageIsStale {

            @Test
            void willIgnoreMessage() {
                leader.handle(new AppendEntriesResponse<>(PREVIOUS_TERM, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
                verifyNoMoreInteractions(otherServerLogReplicator, log);
            }
        }

        @Nested
        class WhenResultIsSuccess {

            private static final int OTHER_SERVER_MATCH_INDEX = 3;

            @BeforeEach
            void setUp() {
                when(otherServerLogReplicator.getMatchIndex()).thenReturn(OTHER_SERVER_MATCH_INDEX);
            }

            @Test
            void willLogSuccessWithReplicatorThenUpdateCommitIndex() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
                InOrder inOrder = inOrder(otherServerLogReplicator, log);
                inOrder.verify(otherServerLogReplicator).logSuccessResponse(5);
                inOrder.verify(log).updateCommitIndex(List.of(OTHER_SERVER_MATCH_INDEX), CURRENT_TERM);
            }

            @Nested
            class AndALeadershipTransferIsInProgress {

                @BeforeEach
                void setUp() {
                    when(leadershipTransfer.isInProgress()).thenReturn(true);
                }

                @Test
                void willSendTimeoutNowMessageIfReadyToTransfer() {
                    leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(LAST_LOG_INDEX)));

                    verify(leadershipTransfer).sendTimeoutNowRequestIfReadyToTransfer();
                }
            }

            @Nested
            class AndTheCommitIndexIsAdvanced {

                @BeforeEach
                void setUp() {
                    when(log.updateCommitIndex(anyList(), any(Term.class))).thenReturn(Optional.of(5));
                }

                @Test
                void willLogSuccessWithReplicatorThenReplicate() {
                    leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));
                    InOrder inOrder = inOrder(otherServerLogReplicator, log);
                    inOrder.verify(otherServerLogReplicator).logSuccessResponse(5);
                    inOrder.verify(otherServerLogReplicator).replicate();
                }
            }

            @Nested
            class AndTheCommitIndexIsNotAdvanced {

                @BeforeEach
                void setUp() {
                    when(log.updateCommitIndex(anyList(), any(Term.class))).thenReturn(Optional.empty());
                }

                @Nested
                class AndSourceNextIndexIsBeforeEndOfLog {

                    @BeforeEach
                    void setUp() {
                        when(log.getLastLogIndex()).thenReturn(6);
                        when(otherServerLogReplicator.getNextIndex()).thenReturn(5);
                    }

                    @Test
                    void willLogSuccessWithReplicatorThenReplicate() {
                        leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(4)));
                        InOrder inOrder = inOrder(otherServerLogReplicator, log);
                        inOrder.verify(otherServerLogReplicator).logSuccessResponse(4);
                        inOrder.verify(otherServerLogReplicator).replicate();
                    }
                }

                @Nested
                class AndSourceNextIndexIsAtEndOfLog {

                    @BeforeEach
                    void setUp() {
                        when(log.getLastLogIndex()).thenReturn(5);
                        when(otherServerLogReplicator.getNextIndex()).thenReturn(6);
                    }

                    @Test
                    void willNotReplicate() {
                        leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, SERVER_ID, true, Optional.of(5)));

                        verify(otherServerLogReplicator, never()).replicate();
                    }
                }
            }
        }

        @Nested
        class WhenResultIsFail {

            @Test
            void willNotUpdateCommitIndex() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
                verify(log, never()).updateCommitIndex(anyList(), any(Term.class));
            }

            @Test
            void willLogFailureWithReplicatorThenReplicate() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, SERVER_ID, false, Optional.empty()));
                InOrder sequence = inOrder(otherServerLogReplicator);
                sequence.verify(otherServerLogReplicator).logFailedResponse();
                sequence.verify(otherServerLogReplicator).replicate();
            }
        }
    }

    @Nested
    class HandleRegisterClientRequest {

        @Nested
        class WhenWeAreNotTransferringLeadership {

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
                        singletonList(new ClientRegistrationEntry(CURRENT_TERM, NEXT_LOG_INDEX)));
            }

            @Test
            void willTriggerReplication() {
                leader.handle(new RegisterClientRequest<>(SERVER_ID));

                verify(otherServerLogReplicator).replicate();
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
        class WhenWeAreTransferringLeadership {

            @BeforeEach
            void setUp() {
                when(leadershipTransfer.isInProgress()).thenReturn(true);
            }

            @Test
            void willRejectWithNotLeaderAndNoLeaderHint() throws ExecutionException, InterruptedException {
                assertThat(leader.handle(new RegisterClientRequest<>(SERVER_ID)).get())
                        .usingRecursiveComparison()
                        .isEqualTo(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, null));
            }
        }
    }

    @Nested
    class HandleClientRequestRequest {

        public static final int SEQUENCE_NUMBER = 0;

        @Nested
        class WhenWeAreTransferringLeadership {

            @BeforeEach
            void setUp() {
                when(leadershipTransfer.isInProgress()).thenReturn(true);
            }

            @Test
            void willRejectWithNotLeaderAndNoLeaderHint() throws ExecutionException, InterruptedException {
                assertThat(leader.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, SEQUENCE_NUMBER, COMMAND)).get())
                        .usingRecursiveComparison()
                        .isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.NOT_LEADER, null, null));
            }
        }

        @Nested
        class WhenClientHasNoActiveSession {

            @BeforeEach
            void setUp() {
                when(clientSessionStore.hasSession(CLIENT_ID)).thenReturn(false);
            }

            @Test
            void willReturnSessionExpiredResponse() throws ExecutionException, InterruptedException {
                assertThat(leader.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, SEQUENCE_NUMBER, COMMAND)).get())
                        .usingRecursiveComparison().isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.SESSION_EXPIRED,
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
                        singletonList(new StateMachineCommandEntry(CURRENT_TERM, CLIENT_ID, SEQUENCE_NUMBER, COMMAND)));
            }

            @Test
            void willTriggerReplication() {
                leader.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, SEQUENCE_NUMBER, COMMAND));

                verify(otherServerLogReplicator).replicate();
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

    @Nested
    class HandleTransferLeadershipMessage {

        @Test
        void willStartLeadershipTransfer() {
            leader.handle(new TransferLeadershipMessage<>(CURRENT_TERM, SERVER_ID));

            verify(leadershipTransfer).start();
        }
    }
}