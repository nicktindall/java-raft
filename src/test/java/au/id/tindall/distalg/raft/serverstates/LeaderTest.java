package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingClientRequestResponse;
import au.id.tindall.distalg.raft.client.responses.PendingRegisterClientResponse;
import au.id.tindall.distalg.raft.client.responses.PendingResponse;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.processors.ProcessorManager;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.rpc.clustermembership.AbdicateLeadershipRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AbdicateLeadershipResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesResponse;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotResponse;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManager;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransfer;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderTest {

    private static final long SERVER_ID = 111;
    private static final long OTHER_SERVER_ID = 112;
    private static final int LAST_RESPONSE_RECEIVED = -1;
    private static final Term PREVIOUS_TERM = new Term(1);
    private static final Term CURRENT_TERM = new Term(2);
    private static final int NEXT_LOG_INDEX = 4;
    private static final int LAST_LOG_INDEX = 3;
    private static final int CLIENT_ID = 55;
    private static final byte[] COMMAND = "TheCommand".getBytes();

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private PendingResponseRegistry pendingResponseRegistry;
    @Mock
    private Log log;
    @Mock
    private ServerStateFactory<Long> serverStateFactory;
    @Mock
    private ClientSessionStore clientSessionStore;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private LeadershipTransfer<Long> leadershipTransfer;
    @Mock
    private ReplicationManager<Long> replicationManager;
    @Mock
    private ClusterMembershipChangeManager<Long> clusterMembershipChangeManager;
    @Mock
    private ProcessorManager processorManager;
    @Mock
    private ElectionScheduler electionScheduler;

    private Leader<Long> leader;

    @BeforeEach
    void setUp() {
        lenient().when(persistentState.getId()).thenReturn(SERVER_ID);
        lenient().when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);
        lenient().when(log.getNextLogIndex()).thenReturn(NEXT_LOG_INDEX);
        leader = new Leader<>(persistentState, log, cluster, pendingResponseRegistry, serverStateFactory, replicationManager,
                clientSessionStore, leadershipTransfer, clusterMembershipChangeManager, processorManager, electionScheduler);
    }

    @Nested
    class OnEnterState {

        @BeforeEach
        void setUp() {
            leader.enterState();
        }

        @Test
        void willStartReplicators() {
            verify(replicationManager).start(processorManager);
        }

        @Test
        void willReplicateToEveryoneToClaimLeadership() {
            verify(replicationManager).replicate();
        }

        @Test
        void willStartClusterMembershipChangeManagerListeningToCommittedEntries() {
            verify(log).addEntryCommittedEventHandler(clusterMembershipChangeManager);
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
            verify(replicationManager).stop();
        }

        @Test
        void willDisposeOfPendingResponseRegistry() {
            verify(pendingResponseRegistry).dispose();
        }

        @Test
        void willStopClusterMembershipChangeManagerListeningToCommittedEntries() {
            verify(log).removeEntryCommittedEventHandler(clusterMembershipChangeManager);
        }

        @Test
        void willCloseClusterMembershipChangeManager() {
            verify(clusterMembershipChangeManager).close();
        }
    }

    @Nested
    class HandleAppendEntriesResponse {

        @Nested
        class WhenMessageIsStale {

            @Test
            void willIgnoreMessage() {
                leader.handle(new AppendEntriesResponse<>(PREVIOUS_TERM, OTHER_SERVER_ID, true, Optional.of(5)));
                verifyNoMoreInteractions(replicationManager, log, electionScheduler);
            }
        }

        @Nested
        class WhenResultIsSuccess {

            private static final int OTHER_SERVER_MATCH_INDEX = 3;

            @BeforeEach
            void setUp() {
                when(replicationManager.getFollowerMatchIndices()).thenReturn(List.of(OTHER_SERVER_MATCH_INDEX));
            }

            @Test
            void willLogSuccessWithReplicatorThenUpdateCommitIndex() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, true, Optional.of(5)));
                InOrder inOrder = inOrder(replicationManager, log);
                inOrder.verify(replicationManager).logSuccessResponse(OTHER_SERVER_ID, 5);
                inOrder.verify(log).updateCommitIndex(List.of(OTHER_SERVER_MATCH_INDEX), CURRENT_TERM);
            }

            @Test
            void willLogMessageFromFollowerWithClusterMembershipChangeManager() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, true, Optional.of(5)));
                verify(clusterMembershipChangeManager).logMessageFromFollower(OTHER_SERVER_ID);
            }

            @Test
            void willUpdateHeartbeat() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, true, Optional.of(5)));
                verify(electionScheduler).updateHeartbeat();
            }

            @Nested
            class AndALeadershipTransferIsInProgress {

                @BeforeEach
                void setUp() {
                    when(leadershipTransfer.isInProgress()).thenReturn(true);
                }

                @Test
                void willSendTimeoutNowMessageIfReadyToTransfer() {
                    leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, true, Optional.of(LAST_LOG_INDEX)));

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
                void willLogSuccessWithReplicatorThenReplicateToAll() {
                    leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, true, Optional.of(5)));
                    InOrder inOrder = inOrder(replicationManager, log);
                    inOrder.verify(replicationManager).logSuccessResponse(OTHER_SERVER_ID, 5);
                    inOrder.verify(replicationManager).replicate();
                }
            }

            @Nested
            class AndTheCommitIndexIsNotAdvanced {

                @BeforeEach
                void setUp() {
                    when(log.updateCommitIndex(anyList(), any(Term.class))).thenReturn(Optional.empty());
                    when(log.getLastLogIndex()).thenReturn(LAST_LOG_INDEX);
                }

                @Test
                void willLogSuccessWithReplicatorThenReplicateIfTrailing() {
                    leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, true, Optional.of(4)));
                    InOrder inOrder = inOrder(replicationManager, log);
                    inOrder.verify(replicationManager).logSuccessResponse(OTHER_SERVER_ID, 4);
                    inOrder.verify(replicationManager).replicateIfTrailingIndex(OTHER_SERVER_ID, LAST_LOG_INDEX);
                }
            }
        }

        @Nested
        class WhenResultIsFail {

            @Test
            void willNotUpdateCommitIndex() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, false, Optional.empty()));
                verify(log, never()).updateCommitIndex(anyList(), any(Term.class));
            }

            @Test
            void willLogFailureWithReplicatorThenReplicate() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, false, Optional.empty()));
                InOrder sequence = inOrder(replicationManager);
                sequence.verify(replicationManager).logFailedResponse(OTHER_SERVER_ID, null);
                sequence.verify(replicationManager).replicate(OTHER_SERVER_ID);
            }

            @Test
            void willLogMessageFromFollowerWithClusterMembershipChangeManager() {
                leader.handle(new AppendEntriesResponse<>(CURRENT_TERM, OTHER_SERVER_ID, false, Optional.empty()));
                verify(clusterMembershipChangeManager).logMessageFromFollower(OTHER_SERVER_ID);
            }
        }
    }

    @Nested
    class HandleInstallSnapshotResponse {

        @Nested
        class WhenResponseIsStale {

            @Test
            void willIgnoreMessage() {
                assertThat(leader.handle(new InstallSnapshotResponse<>(PREVIOUS_TERM, OTHER_SERVER_ID, true, 123, 456)))
                        .usingRecursiveComparison()
                        .isEqualTo(complete(leader));
                verifyNoMoreInteractions(replicationManager, log, electionScheduler);
            }
        }

        @Nested
        class WhenResultIsSuccess {

            @BeforeEach
            void setUp() {
                assertThat(leader.handle(new InstallSnapshotResponse<>(CURRENT_TERM, OTHER_SERVER_ID, true, 123, 456)))
                        .usingRecursiveComparison()
                        .isEqualTo(complete(leader));
            }

            @Test
            void willLogMessageFromFollowerWithClusterMembershipChangeManager() {
                verify(clusterMembershipChangeManager).logMessageFromFollower(OTHER_SERVER_ID);
            }

            @Test
            void willNotifyReplicationManagerOfSuccessAndTriggerReplicationToFollower() {
                final InOrder inOrder = inOrder(replicationManager);
                inOrder.verify(replicationManager).logSuccessSnapshotResponse(OTHER_SERVER_ID, 123, 456);
                inOrder.verify(replicationManager).replicate(OTHER_SERVER_ID);
            }

            @Test
            void willUpdateHeartbeat() {
                verify(electionScheduler).updateHeartbeat();
            }
        }

        @Nested
        class WhenResultIsFailure {

            @BeforeEach
            void setUp() {
                assertThat(leader.handle(new InstallSnapshotResponse<>(CURRENT_TERM, OTHER_SERVER_ID, false, 123, 456)))
                        .usingRecursiveComparison()
                        .isEqualTo(complete(leader));
            }

            @Test
            void willLogMessageFromFollowerWithClusterMembershipChangeManager() {
                verify(clusterMembershipChangeManager).logMessageFromFollower(OTHER_SERVER_ID);
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
                leader.handle(new RegisterClientRequest<>());

                ArgumentCaptor<List<LogEntry>> captor = ArgumentCaptor.forClass(List.class);
                verify(log).appendEntries(eq(LAST_LOG_INDEX), captor.capture());
                assertThat(captor.getValue()).usingFieldByFieldElementComparator().isEqualTo(
                        singletonList(new ClientRegistrationEntry(CURRENT_TERM, NEXT_LOG_INDEX)));
            }

            @Test
            void willTriggerReplication() {
                leader.handle(new RegisterClientRequest<>());

                verify(replicationManager).replicate();
            }

            @Test
            @SuppressWarnings("unchecked")
            void willRegisterAnOutstandingResponseAndReturnAssociatedPromise() {
                CompletableFuture<RegisterClientResponse<Long>> result = leader.handle(new RegisterClientRequest<>());

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
                assertThat(leader.handle(new RegisterClientRequest<>()).get())
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
                assertThat(leader.handle(new ClientRequestRequest<>(CLIENT_ID, SEQUENCE_NUMBER, LAST_RESPONSE_RECEIVED, COMMAND)).get())
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
                assertThat(leader.handle(new ClientRequestRequest<>(CLIENT_ID, SEQUENCE_NUMBER, LAST_RESPONSE_RECEIVED, COMMAND)).get())
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
                leader.handle(new ClientRequestRequest<>(CLIENT_ID, SEQUENCE_NUMBER, LAST_RESPONSE_RECEIVED, COMMAND));

                ArgumentCaptor<List<LogEntry>> captor = ArgumentCaptor.forClass(List.class);
                verify(log).appendEntries(eq(LAST_LOG_INDEX), captor.capture());
                assertThat(captor.getValue()).usingFieldByFieldElementComparator().isEqualTo(
                        singletonList(new StateMachineCommandEntry(CURRENT_TERM, CLIENT_ID, LAST_RESPONSE_RECEIVED, SEQUENCE_NUMBER, COMMAND)));
            }

            @Test
            void willTriggerReplication() {
                leader.handle(new ClientRequestRequest<>(CLIENT_ID, SEQUENCE_NUMBER, LAST_RESPONSE_RECEIVED, COMMAND));

                verify(replicationManager).replicate();
            }

            @Test
            @SuppressWarnings("unchecked")
            void willRegisterAnOutstandingResponseAndReturnAssociatedPromise() {
                CompletableFuture<ClientRequestResponse<Long>> result = leader.handle(new ClientRequestRequest<>(CLIENT_ID, SEQUENCE_NUMBER, LAST_RESPONSE_RECEIVED, COMMAND));

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

    @Nested
    class HandleAddServerRequest {

        @Mock
        private CompletableFuture<AddServerResponse<Long>> responseFuture;

        @Test
        void willDelegateToClusterMembershipManager() {
            when(clusterMembershipChangeManager.addServer(OTHER_SERVER_ID)).thenReturn(responseFuture);

            assertThat(leader.handle(new AddServerRequest<>(OTHER_SERVER_ID))).isSameAs(responseFuture);
        }
    }

    @Nested
    class HandleRemoveServerRequest {

        @Mock
        private CompletableFuture<RemoveServerResponse<Long>> responseFuture;

        @Test
        void willDelegateToClusterMembershipManager() {
            when(clusterMembershipChangeManager.removeServer(OTHER_SERVER_ID)).thenReturn(responseFuture);

            assertThat(leader.handle(new RemoveServerRequest<>(OTHER_SERVER_ID))).isSameAs(responseFuture);
        }

        @Test
        void willRejectRequestIfServerIdIsLeaderId() {
            assertThat(leader.handle(new RemoveServerRequest<>(SERVER_ID))).hasFailedWithThrowableThat()
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class HandleAbdicateLeadershipRequest {

        @Test
        void willStartLeadershipTransferAndReturnOK() {
            CompletableFuture<AbdicateLeadershipResponse<Long>> handle = leader.handle(new AbdicateLeadershipRequest<>());

            assertThat(handle).isCompletedWithValue(AbdicateLeadershipResponse.getOK());
            verify(leadershipTransfer).start();
        }
    }
}