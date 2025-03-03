package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.rpc.server.TransferLeadershipMessage;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotResponse;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.NOT_LEADER;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerStateTest {

    private static final long SERVER_ID = 100;
    private static final long OTHER_SERVER_ID = 101;
    private static final long LEADER_ID = 102;
    private static final int CLIENT_ID = 200;
    private static final int LAST_RESPONSE_RECEIVED = -1;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final Term TERM_3 = new Term(3);
    private static final LogEntry ENTRY_1 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 1, "second".getBytes());
    private static final LogEntry ENTRY_3 = new StateMachineCommandEntry(TERM_1, CLIENT_ID, -1, 2, "third".getBytes());

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private RpcMessage<Long> rpcMessage;
    @Mock
    private ServerStateFactory<Long> serverStateFactory;
    @Mock
    private Follower<Long> follower;
    @Mock
    private Log log;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private ElectionScheduler electionScheduler;

    @Nested
    class HandleRequest {

        @Test
        void willRevertToFollowerStateAndResetVotedForAndAdvanceTerm_WhenSenderTermIsGreaterThanLocalTerm() {
            when(persistentState.getCurrentTerm()).thenReturn(TERM_1);
            when(serverStateFactory.createFollower(OTHER_SERVER_ID)).thenReturn(follower);
            when(rpcMessage.getTerm()).thenReturn(TERM_2);
            when(rpcMessage.getSource()).thenReturn(OTHER_SERVER_ID);

            var serverState = new MinimalServerState(persistentState, log, cluster, serverStateFactory, LEADER_ID, electionScheduler);
            Result<Long> result = serverState.handle(rpcMessage);

            assertThat(result).usingRecursiveComparison().isEqualTo(incomplete(follower));
        }

        @Nested
        class WhenHeartbeatIsCurrent {

            @BeforeEach
            void setUp() {
                when(electionScheduler.isHeartbeatCurrent()).thenReturn(true);
            }

            @Test
            public void willRejectVoteRequest_WhenElectionIsNotEarly() {
                when(persistentState.getCurrentTerm()).thenReturn(TERM_2);
                var serverState = new MinimalServerState(persistentState, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, null, electionScheduler);
                assertThat(serverState.handle((RpcMessage<Long>) new RequestVoteRequest<>(TERM_3, OTHER_SERVER_ID, 3, Optional.of(TERM_1), false)))
                        .usingRecursiveComparison()
                        .isEqualTo(complete(serverState));

                verify(cluster).sendRequestVoteResponse(TERM_2, OTHER_SERVER_ID, false);
                verify(persistentState, never()).setVotedFor(any());
            }

            @Test
            public void willTransitionToFollower_WhenElectionIsEarly() {
                when(persistentState.getCurrentTerm()).thenReturn(TERM_2);
                when(serverStateFactory.createFollower(OTHER_SERVER_ID)).thenReturn(follower);
                var serverState = new MinimalServerState(persistentState, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, null, electionScheduler);
                assertThat(serverState.handle((RpcMessage<Long>) new RequestVoteRequest<>(TERM_3, OTHER_SERVER_ID, 3, Optional.of(TERM_1), true)))
                        .usingRecursiveComparison()
                        .isEqualTo(incomplete(follower));

                verify(persistentState).setCurrentTerm(TERM_3);
            }
        }
    }

    @Nested
    class HandleRequestVoteRequest {

        @Test
        void willNotGrantVote_WhenRequesterTermIsLowerThanLocalTerm() {
            when(persistentState.getCurrentTerm()).thenReturn(TERM_3);
            var serverState = new MinimalServerState(persistentState, log, cluster, serverStateFactory, null, electionScheduler);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 100, Optional.of(TERM_2), false));

            verify(cluster).sendRequestVoteResponse(TERM_2, OTHER_SERVER_ID, false);
            verify(persistentState, never()).setVotedFor(anyLong());
        }

        @Test
        void willNotGrantVote_WhenServerHasAlreadyVoted() {
            when(persistentState.getCurrentTerm()).thenReturn(TERM_3);
            when(persistentState.getVotedFor()).thenReturn(Optional.of(SERVER_ID));
            var serverState = new MinimalServerState(persistentState, log, cluster, serverStateFactory, null, electionScheduler);
            serverState.handle(new RequestVoteRequest<>(TERM_3, OTHER_SERVER_ID, 100, Optional.of(TERM_2), false));

            verify(cluster).sendRequestVoteResponse(TERM_3, OTHER_SERVER_ID, false);
            verify(persistentState, never()).setVotedFor(anyLong());
        }

        @Test
        void willNotGrantVote_WhenServerLogIsMoreUpToDateThanRequesterLog() {
            when(persistentState.getCurrentTerm()).thenReturn(TERM_2);
            var serverState = new MinimalServerState(persistentState, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, null, electionScheduler);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 2, Optional.of(TERM_0), false));

            verify(cluster).sendRequestVoteResponse(TERM_2, OTHER_SERVER_ID, false);
            verify(persistentState, never()).setVotedFor(anyLong());
        }

        @Test
        void willGrantVote_WhenRequesterTermIsEqualLogsAreSameAndServerHasNotAlreadyVoted() {
            when(persistentState.getCurrentTerm()).thenReturn(TERM_2);
            var serverState = new MinimalServerState(persistentState, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, null, electionScheduler);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1), false));

            verify(cluster).sendRequestVoteResponse(TERM_2, OTHER_SERVER_ID, true);
            verify(persistentState).setVotedFor(OTHER_SERVER_ID);
        }

        @Test
        void willGrantVote_WhenRequesterTermIsEqualServerLogIsLessUpToDateAndServerHasNotAlreadyVoted() {
            when(persistentState.getCurrentTerm()).thenReturn(TERM_2);
            var serverState = new MinimalServerState(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, null, electionScheduler);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1), false));

            verify(cluster).sendRequestVoteResponse(TERM_2, OTHER_SERVER_ID, true);
            verify(persistentState).setVotedFor(OTHER_SERVER_ID);
        }

        @Test
        void willGrantVote_WhenRequesterTermIsEqualLogsAreSameAndServerHasAlreadyVotedForRequester() {
            when(persistentState.getCurrentTerm()).thenReturn(TERM_2);
            when(persistentState.getVotedFor()).thenReturn(Optional.of(OTHER_SERVER_ID));
            var serverState = new MinimalServerState(persistentState, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, null, electionScheduler);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1), false));

            verify(cluster).sendRequestVoteResponse(TERM_2, OTHER_SERVER_ID, true);
            verify(persistentState).setVotedFor(OTHER_SERVER_ID);
        }
    }

    @Nested
    class HandleRegisterClientRequest {

        @Test
        void willReturnNotLeader() throws ExecutionException, InterruptedException {
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);
            CompletableFuture<RegisterClientResponse<Long>> response = serverState.handle(new RegisterClientRequest<>(SERVER_ID));

            assertThat(response).isCompleted();
            assertThat(response.get()).usingRecursiveComparison()
                    .isEqualTo(new RegisterClientResponse<>(NOT_LEADER, null, LEADER_ID));
        }
    }

    @Nested
    class HandleClientRequestRequest {

        @Test
        void willReturnNotLeader() throws ExecutionException, InterruptedException {
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);
            CompletableFuture<ClientRequestResponse<Long>> response = serverState.handle(new ClientRequestRequest<>(SERVER_ID, CLIENT_ID, 0, LAST_RESPONSE_RECEIVED, "command".getBytes(StandardCharsets.UTF_8)));

            assertThat(response).isCompleted();
            assertThat(response.get()).usingRecursiveComparison()
                    .isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.NOT_LEADER, null, LEADER_ID));
        }
    }

    @Nested
    class HandleTimeoutNowMessage {

        @Mock
        private Candidate<Long> candidate;
        @Mock
        private PersistentState<Long> persistentState;

        @Test
        void willTransitionToCandidateStateAndContinueHandlingMessage() {
            when(serverStateFactory.createCandidate()).thenReturn(candidate);
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);

            assertThat(serverState.handle(new TimeoutNowMessage<>(TERM_0, SERVER_ID))).usingRecursiveComparison().isEqualTo(incomplete(candidate));
        }
    }

    @Nested
    class HandleTransferLeadershipMessage {

        @Test
        void willDoNothing() {
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);

            assertThat(serverState.handle(new TransferLeadershipMessage<>(TERM_0, SERVER_ID))).usingRecursiveComparison().isEqualTo(complete(serverState));
        }
    }

    @Nested
    class HandleAddServerRequest {

        @Test
        void willReturnNotLeader() throws ExecutionException, InterruptedException {
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);

            var response = serverState.handle(new AddServerRequest<>(1234L));
            assertThat(response).isCompleted();
            assertThat(response.get()).usingRecursiveComparison()
                    .isEqualTo(AddServerResponse.NOT_LEADER);
        }
    }

    @Nested
    class HandleRemoveServerRequest {

        @Test
        void willReturnNotLeader() throws ExecutionException, InterruptedException {
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);

            var response = serverState.handle(new RemoveServerRequest<>(1234L));
            assertThat(response).isCompleted();
            assertThat(response.get()).usingRecursiveComparison()
                    .isEqualTo(RemoveServerResponse.NOT_LEADER);
        }
    }

    @Nested
    class HandleInstallSnapshotRequest {

        @Test
        void willDoNothing() {
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);

            assertThat(serverState.handle(new InstallSnapshotRequest<>(TERM_0, LEADER_ID, SERVER_ID, 999, TERM_2, null, 101010, 1234, "Hello".getBytes(), true)))
                    .usingRecursiveComparison().isEqualTo(complete(serverState));
        }
    }

    @Nested
    class HandleInstallSnapshotResponse {

        @Test
        void willDoNothing() {
            var serverState = new MinimalServerState(persistentState, logContaining(), cluster, serverStateFactory, LEADER_ID, electionScheduler);

            assertThat(serverState.handle(new InstallSnapshotResponse<>(TERM_0, LEADER_ID, SERVER_ID, true, 999, 1)))
                    .usingRecursiveComparison().isEqualTo(complete(serverState));
        }
    }

    private static class MinimalServerState extends ServerStateImpl<Long> {

        MinimalServerState(PersistentState<Long> persistentState, Log log, Cluster<Long> cluster, ServerStateFactory<Long> serverStateFactory, Long leaderId, ElectionScheduler electionScheduler) {
            super(persistentState, log, cluster, serverStateFactory, leaderId, electionScheduler);
        }

        @Override
        public ServerStateType getServerStateType() {
            // Just return something it's used in a log message
            return ServerStateType.LEADER;
        }
    }
}
