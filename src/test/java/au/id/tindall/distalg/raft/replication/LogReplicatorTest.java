package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.log.storage.InMemoryLogStorage;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SKIPPED;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SUCCESS;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SWITCH_TO_SNAPSHOT_REPLICATION;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class LogReplicatorTest {

    private static final long FOLLOWER_ID = 456L;
    private static final int INITIAL_NEXT_INDEX = 5;
    private static final Term CURRENT_TERM = new Term(3);
    private static final int COMMIT_INDEX = 3;
    private static final int LAST_LOG_INDEX = 4;
    private static final int CLIENT_ID = 123;
    private static final LogEntry ENTRY_ONE = new StateMachineCommandEntry(new Term(0), CLIENT_ID, -1, 0, "one".getBytes());
    private static final LogEntry ENTRY_TWO = new StateMachineCommandEntry(new Term(1), CLIENT_ID, -1, 1, "two".getBytes());
    private static final LogEntry ENTRY_THREE = new StateMachineCommandEntry(new Term(2), CLIENT_ID, -1, 2, "three".getBytes());
    private static final LogEntry ENTRY_FOUR = new StateMachineCommandEntry(new Term(2), CLIENT_ID, -1, 3, "four".getBytes());
    private static final LogEntry ENTRY_FIVE = new StateMachineCommandEntry(new Term(2), CLIENT_ID, -1, 4, "five".getBytes());
    private static final int MAX_BATCH_SIZE = 1;

    private LogReplicator<Long> logReplicator;

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private MatchIndexAdvancedListener<Long> matchIndexAdvancedListener;

    private Log log;

    @BeforeEach
    void setUp() {
        log = logContaining(ENTRY_ONE, ENTRY_TWO, ENTRY_THREE, ENTRY_FOUR);
        log.advanceCommitIndex(COMMIT_INDEX);
        logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, INITIAL_NEXT_INDEX, matchIndexAdvancedListener));
    }

    @Nested
    class SendNextReplicationMessage {

        @Test
        void willSendEmptyAppendEntriesRequest_WhenThereAreNoLogEntries() {
            logReplicator = new LogReplicator<>(logContaining(), CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, 1, matchIndexAdvancedListener));
            assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, 0, Optional.empty(), emptyList(), 0);
        }

        @Test
        void shouldSendEmptyAppendEntriesRequest_WhenFollowerIsCaughtUp() {
            assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
        }

        @Test
        void shouldSendUpToMaxBatchSizeEntries_WhenFollowerIsLagging() {
            int maxBatchSize = 2;
            logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, maxBatchSize, new ReplicationState<>(FOLLOWER_ID, LAST_LOG_INDEX - 2, matchIndexAdvancedListener));
            assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX - 3, Optional.of(ENTRY_ONE.getTerm()), List.of(ENTRY_TWO, ENTRY_THREE), COMMIT_INDEX);
        }

        @Nested
        class WhenNotForced {

            @Test
            void willNotResendSameExcerpts() {
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SKIPPED);
                verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
                verifyNoMoreInteractions(cluster);
            }

            @Test
            void willSendSameExcerptsWhenCommitIndexAdvances() {
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                log.advanceCommitIndex(COMMIT_INDEX + 1);
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
                verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX + 1);
                verifyNoMoreInteractions(cluster);
            }

            @Test
            void willSendNewExcerptsWhenLogIsAppended() {
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                log.appendEntries(LAST_LOG_INDEX, List.of(ENTRY_FIVE));
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
                verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), List.of(ENTRY_FIVE), COMMIT_INDEX);
                verifyNoMoreInteractions(cluster);
            }
        }

        @Nested
        class WhenForced {

            @Test
            void willResendSameExcerpts() {
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                assertThat(logReplicator.sendNextReplicationMessage(true)).isEqualTo(SUCCESS);
                verify(cluster, times(2)).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
                verifyNoMoreInteractions(cluster);
            }
        }

        @Nested
        class WhenSomeOfTheLogHasBeenTruncated {

            @BeforeEach
            void setUp() {
                final InMemoryLogStorage logStorage = new InMemoryLogStorage();
                log = new Log(logStorage);
                log.appendEntries(0, List.of(ENTRY_ONE, ENTRY_TWO, ENTRY_THREE, ENTRY_FOUR));
                log.advanceCommitIndex(COMMIT_INDEX);
                logStorage.installSnapshot(new InMemorySnapshot(3, ENTRY_THREE.getTerm(), null));
            }

            @Test
            void willSwitchToSnapshotReplicationWhenNextIndexHasBeenTruncated() {
                logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, 2, matchIndexAdvancedListener));
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SWITCH_TO_SNAPSHOT_REPLICATION);
                verifyNoInteractions(cluster);
            }

            @Test
            void willSendAppendEntriesEventWhenPrevIndexHasBeenTruncated() {
                logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, 4, matchIndexAdvancedListener));
                assertThat(logReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, 3, Optional.of(ENTRY_THREE.getTerm()),
                        List.of(ENTRY_FOUR), COMMIT_INDEX);
            }
        }
    }
}