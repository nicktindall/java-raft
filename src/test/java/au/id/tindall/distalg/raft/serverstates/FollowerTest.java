package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.snapshots.InstallSnapshotRequest;
import au.id.tindall.distalg.raft.state.InMemoryPersistentState;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static au.id.tindall.distalg.raft.DomainUtils.createInMemorySnapshot;
import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FollowerTest {

    private static final long SERVER_ID = 100L;
    private static final long LEADER_SERVER_ID = 101L;
    private static final long NON_LEADER_SERVER_ID = 102L;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final int CLIENT_ID = 123;
    private static final LogEntry ENTRY_1 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, -1, 1, "second".getBytes());
    private static final LogEntry ENTRY_3 = new StateMachineCommandEntry(TERM_1, CLIENT_ID, -1, 2, "third".getBytes());
    private static final LogEntry ENTRY_4 = new StateMachineCommandEntry(TERM_1, CLIENT_ID, -1, 3, "fourth".getBytes());
    private static final LogEntry ENTRY_5 = new StateMachineCommandEntry(TERM_1, CLIENT_ID, -1, 4, "fifth".getBytes());

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private ServerStateFactory<Long> serverStateFactory;
    @Mock
    private ElectionScheduler electionScheduler;
    @Mock
    private PersistentState<Long> persistentState;

    @Nested
    class OnEnterState {

        Follower<Long> followerState;

        @BeforeEach
        void setUp() {
            followerState = new Follower<>(persistentState, logContaining(), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            followerState.enterState();
        }

        @Test
        void willStartElectionTimeouts() {
            verify(electionScheduler).startTimeouts();
        }
    }

    @Nested
    class OnLeaveState {

        Follower<Long> followerState;
        @Mock
        Snapshot currentSnapshot;

        @BeforeEach
        void setUp() {
            followerState = new Follower<>(persistentState, logContaining(), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            lenient().when(persistentState.getCurrentTerm()).thenReturn(TERM_1);
            lenient().when(currentSnapshot.getLastIndex()).thenReturn(999);
            lenient().when(currentSnapshot.getLastTerm()).thenReturn(TERM_0);
        }

        @Test
        void willStartElectionTimeouts() {
            followerState.leaveState();
            verify(electionScheduler).stopTimeouts();
        }

        @Test
        void willDeleteAnyPartialSnapshots() throws IOException {
            when(persistentState.createSnapshot(anyInt(), any(Term.class), any(), anyInt()))
                    .thenAnswer(iom -> currentSnapshot);
            followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 999, TERM_0, null, 2, 0, "test".getBytes(), false));
            followerState.leaveState();
            verify(currentSnapshot).delete();
        }
    }

    @Nested
    class HandleAppendEntriesRequest {

        @BeforeEach
        void setUp() {
            lenient().when(persistentState.getCurrentTerm()).thenReturn(TERM_1);
        }

        @Test
        void willRejectRequest_WhenLeaderTermIsLessThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_0, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), emptyList(), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_0, LEADER_SERVER_ID, false, Optional.empty());
            verifyNoInteractions(electionScheduler);
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willRejectRequest_PrevLogEntryHasIncorrectTerm() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_1), emptyList(), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, false, Optional.of(2));
            verify(electionScheduler).resetTimeout();
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willRejectRequest_WhenPrevLogEntryIsAfterEndOfLog() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 4, Optional.of(TERM_1), singletonList(ENTRY_5), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, false, Optional.of(3));
            verify(electionScheduler).resetTimeout();
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willRejectRequest_WhenPrevLogEntryIsBeforeStartOfLog() {
            final InMemoryPersistentState<Long> persistentState = new InMemoryPersistentState<>(SERVER_ID);
            persistentState.setCurrentTerm(TERM_1);
            final Log log = new Log(persistentState.getLogStorage());
            log.appendEntries(0, List.of(ENTRY_1, ENTRY_2, ENTRY_3, ENTRY_4, ENTRY_5));
            final InMemorySnapshot nextSnapshot = createInMemorySnapshot(3, TERM_1);
            persistentState.setCurrentSnapshot(nextSnapshot);
            assertThat(log.getPrevIndex()).isEqualTo(3);
            Follower<Long> followerState = new Follower<>(persistentState, log, cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, false, Optional.of(6));
            verify(electionScheduler).resetTimeout();
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willAcceptRequest_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            verify(electionScheduler).resetTimeout();
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willThrowIllegalStateException_WhenLeaderTermIsGreaterThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> followerState.handle(new AppendEntriesRequest<>(TERM_2, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0)));
            verifyNoInteractions(electionScheduler);
            assertThat(ex.getMessage()).isEqualTo("Received a request from a future term! this should never happen");
        }

        @Test
        void willAcceptRequest_AndAdvanceCommitIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLocal() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            Follower<Long> followerState = new Follower<>(persistentState, log, cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 2));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            verify(electionScheduler).resetTimeout();
            assertThat(log.getCommitIndex()).isEqualTo(2);
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willAcceptRequest_AndAdvanceCommitIndexToLastLocalIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLastLocalIndex() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            Follower<Long> followerState = new Follower<>(persistentState, log, cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 10));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            verify(electionScheduler).resetTimeout();
            assertThat(log.getCommitIndex()).isEqualTo(3);
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willReturnLastAppendedIndex_WhenAppendIsSuccessful() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 1, Optional.of(TERM_0), List.of(ENTRY_2), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(2));
            verify(electionScheduler).resetTimeout();
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willRejectRequest_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndSenderIsNotTheCurrentLeader() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, NON_LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, NON_LEADER_SERVER_ID, false, Optional.empty());
            verifyNoInteractions(electionScheduler);
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }
    }

    @Nested
    class HandleInstallSnapshotRequest {

        private final int LAST_INDEX = 1234;
        private final Term LAST_TERM = TERM_0;
        private final byte[] SNAPSHOT_CHUNK = "Test Snapshot Chunk contents".getBytes();
        private final int OTHER_LAST_INDEX = 1111;
        private final byte[] OTHER_SNAPSHOT_CHUNK = "Other snapshot chunk contents".getBytes();

        private List<InMemorySnapshot> createdSnapshots;
        private InMemorySnapshot currentSnapshot;

        @BeforeEach
        void setUp() throws IOException {
            createdSnapshots = new ArrayList<>();
            lenient().when(persistentState.createSnapshot(anyInt(), any(Term.class), any(), anyInt()))
                    .thenAnswer(iom -> {
                        final InMemorySnapshot newSnapshot = new InMemorySnapshot(iom.getArgument(0), iom.getArgument(1), iom.getArgument(2));
                        createdSnapshots.add(newSnapshot);
                        currentSnapshot = newSnapshot;
                        return newSnapshot;
                    });
            when(persistentState.getCurrentTerm()).thenReturn(TERM_1);
        }

        @Test
        void willThrowIllegalStateException_WhenLeaderTermIsGreaterThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            final InstallSnapshotRequest<Long> installSnapshotRequest = new InstallSnapshotRequest<>(TERM_2, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, LAST_TERM, null, 0, 0, SNAPSHOT_CHUNK, false);
            assertThatThrownBy(() -> followerState.handle(installSnapshotRequest))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Received a request from a future term! this should never happen");
            verifyNoInteractions(electionScheduler);
        }

        @Test
        void willRejectRequest_WhenLeaderTermIsLessThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new InstallSnapshotRequest<>(TERM_0, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, LAST_TERM, null, 0, 0, SNAPSHOT_CHUNK, false));
            verify(cluster).sendInstallSnapshotResponse(TERM_0, LEADER_SERVER_ID, false, LAST_INDEX, SNAPSHOT_CHUNK.length);
            verifyNoInteractions(electionScheduler);
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willRejectRequest_WhenFromSomeoneOtherThanTheLeader() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new InstallSnapshotRequest<>(TERM_1, NON_LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, LAST_TERM, null, 0, 0, SNAPSHOT_CHUNK, false));
            verify(cluster).sendInstallSnapshotResponse(TERM_1, NON_LEADER_SERVER_ID, false, LAST_INDEX, SNAPSHOT_CHUNK.length);
            verifyNoInteractions(electionScheduler);
            assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
        }

        @Test
        void willResetElectionTimeoutWhenRequestIsValid() {
            Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, TERM_0, null, 2, 0, SNAPSHOT_CHUNK, false));
            verify(electionScheduler).resetTimeout();
        }

        @Nested
        class WhenSnapshotOffsetIsZero {

            @Nested
            class AndThereIsNoPartialSnapshotReceived {

                @Test
                void willAcceptRequestAndStartWritingNewSnapshot() {
                    Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
                    Result<Long> result = followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, TERM_0, null, 2, 0, SNAPSHOT_CHUNK, false));
                    verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, LAST_INDEX, SNAPSHOT_CHUNK.length - 1);
                    assertThat(currentSnapshot.getContents().array()).startsWith(SNAPSHOT_CHUNK);
                    assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
                }
            }

            @Nested
            class AndThereIsADifferentPartialSnapshotReceived {

                @Test
                void willAcceptRequestAndStartWritingNewSnapshot() {
                    Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
                    followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, OTHER_LAST_INDEX, LAST_TERM, null, 2, 0, OTHER_SNAPSHOT_CHUNK, false));
                    verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, OTHER_LAST_INDEX, OTHER_SNAPSHOT_CHUNK.length - 1);
                    Result<Long> result = followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, LAST_TERM, null, 2, 0, SNAPSHOT_CHUNK, false));
                    verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, LAST_INDEX, SNAPSHOT_CHUNK.length - 1);
                    assertThat(currentSnapshot.getContents().array()).startsWith(SNAPSHOT_CHUNK);
                    assertThat(currentSnapshot.getLastIndex()).isEqualTo(LAST_INDEX);
                    assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
                }
            }
        }

        @Nested
        class WhenSnapshotOffsetIsNonZero {
            @Nested
            class AndThereIsNoPartialSnapshotReceived {
                @Test
                void willIgnoreTheRequest() {
                    Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
                    Result<Long> result = followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, TERM_0, null, 2, 30, SNAPSHOT_CHUNK, false));
                    verifyNoInteractions(cluster);
                    assertThat(currentSnapshot).isNull();
                    assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
                }
            }

            @Nested
            class AndThereIsADifferentPartialSnapshotReceived {

                @Test
                void willRespondWithFailure() {
                    Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
                    followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, OTHER_LAST_INDEX, LAST_TERM, null, 2, 0, OTHER_SNAPSHOT_CHUNK, false));
                    verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, OTHER_LAST_INDEX, OTHER_SNAPSHOT_CHUNK.length - 1);
                    Result<Long> result = followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, LAST_TERM, null, 2, 30, SNAPSHOT_CHUNK, false));
                    verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, false, LAST_INDEX, 30 + SNAPSHOT_CHUNK.length - 1);
                    assertThat(currentSnapshot.getContents().array()).startsWith(OTHER_SNAPSHOT_CHUNK);
                    assertThat(currentSnapshot.getLastIndex()).isEqualTo(OTHER_LAST_INDEX);
                    assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
                }
            }
        }

        @Nested
        class WhenSnapshotIsComplete {

            @Test
            void theSnapshotIsFinalisedAndInstalledToThePersistentState() throws IOException {
                Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
                Result<Long> result = followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, TERM_0, null, 2, 0, SNAPSHOT_CHUNK, true));
                verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, LAST_INDEX, SNAPSHOT_CHUNK.length - 1);
                assertThat(currentSnapshot.getContents().array()).startsWith(SNAPSHOT_CHUNK);
                verify(persistentState).setCurrentSnapshot(currentSnapshot);
                assertThat(result).usingRecursiveComparison().isEqualTo(complete(followerState));
            }

            @Test
            void lateInstallSnapshotRequestsWillBeAcknowledged() throws IOException {
                Follower<Long> followerState = new Follower<>(persistentState, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
                followerState.handle(new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, TERM_0, null, 2, 0, Arrays.copyOfRange(SNAPSHOT_CHUNK, 0, 10), false));
                verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, LAST_INDEX, 9);
                final InstallSnapshotRequest<Long> repeatedFinalRequest = new InstallSnapshotRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, LAST_INDEX, TERM_0, null, 2, 10, Arrays.copyOfRange(SNAPSHOT_CHUNK, 10, SNAPSHOT_CHUNK.length), true);
                followerState.handle(repeatedFinalRequest);
                verify(cluster).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, LAST_INDEX, SNAPSHOT_CHUNK.length - 1);
                verify(persistentState).setCurrentSnapshot(currentSnapshot);
                followerState.handle(repeatedFinalRequest);
                verify(cluster, times(2)).sendInstallSnapshotResponse(TERM_1, LEADER_SERVER_ID, true, LAST_INDEX, SNAPSHOT_CHUNK.length - 1);
                verify(persistentState).setCurrentSnapshot(currentSnapshot);
            }
        }
    }
}