package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LogReplicatorTest {

    private static final long FOLLOWER_ID = 456L;
    private static final int INITIAL_NEXT_INDEX = 5;
    private static final Term CURRENT_TERM = new Term(3);
    private static final int COMMIT_INDEX = 3;
    private static final int LAST_LOG_INDEX = 4;
    private static final int CLIENT_ID = 123;
    private static final LogEntry ENTRY_ONE = new StateMachineCommandEntry(new Term(0), CLIENT_ID, 0, "one".getBytes());
    private static final LogEntry ENTRY_TWO = new StateMachineCommandEntry(new Term(1), CLIENT_ID, 1, "two".getBytes());
    private static final LogEntry ENTRY_THREE = new StateMachineCommandEntry(new Term(2), CLIENT_ID, 2, "three".getBytes());
    private static final LogEntry ENTRY_FOUR = new StateMachineCommandEntry(new Term(2), CLIENT_ID, 3, "four".getBytes());
    private static final int MAX_BATCH_SIZE = 1;

    private LogReplicator<Long> logReplicator;

    @Mock
    private Cluster<Long> cluster;

    private Log log;

    @BeforeEach
    void setUp() {
        log = logContaining(ENTRY_ONE, ENTRY_TWO, ENTRY_THREE, ENTRY_FOUR);
        log.advanceCommitIndex(COMMIT_INDEX);
        logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, FOLLOWER_ID, MAX_BATCH_SIZE, INITIAL_NEXT_INDEX, new SynchronousReplicationScheduler());
        logReplicator.start();
    }

    @Nested
    class Constructor {
        @Test
        void matchIndexWillBeInitializedToZero() {
            assertThat(logReplicator.getMatchIndex()).isZero();
        }

        @Test
        void nextIndexWillBeInitializedToValuePassed() {
            assertThat(logReplicator.getNextIndex()).isEqualTo(INITIAL_NEXT_INDEX);
        }

        @Test
        void willSetSendAppendEntriesRequestOnReplicationScheduler() {
            ReplicationScheduler replicationScheduler = mock(ReplicationScheduler.class);
            new LogReplicator<>(log, CURRENT_TERM, cluster, FOLLOWER_ID, MAX_BATCH_SIZE, INITIAL_NEXT_INDEX, replicationScheduler);
            verify(replicationScheduler).setSendAppendEntriesRequest(any(Runnable.class));
        }
    }

    @Nested
    class Start {

        @Test
        void willStartLogReplicator() {
            ReplicationScheduler replicationScheduler = mock(ReplicationScheduler.class);
            LogReplicator<Long> replicator = new LogReplicator<>(log, CURRENT_TERM, cluster, FOLLOWER_ID, MAX_BATCH_SIZE, INITIAL_NEXT_INDEX, replicationScheduler);
            reset(replicationScheduler);
            replicator.start();
            verify(replicationScheduler).start();
        }
    }

    @Nested
    class Stop {

        @Test
        void willStopLogReplicator() {
            ReplicationScheduler replicationScheduler = mock(ReplicationScheduler.class);
            LogReplicator<Long> replicator = new LogReplicator<>(log, CURRENT_TERM, cluster, FOLLOWER_ID, MAX_BATCH_SIZE, INITIAL_NEXT_INDEX, replicationScheduler);
            reset(replicationScheduler);
            replicator.stop();
            verify(replicationScheduler).stop();
        }
    }

    @Nested
    class LogSuccessResponse {

        @Test
        void willSetNextIndex() {
            int lastAppendedIndex = INITIAL_NEXT_INDEX + 1;
            logReplicator.logSuccessResponse(lastAppendedIndex);
            assertThat(logReplicator.getNextIndex()).isEqualTo(lastAppendedIndex + 1);
        }

        @Test
        void willSetMatchIndex() {
            int lastAppendedIndex = 2;
            logReplicator.logSuccessResponse(lastAppendedIndex);
            assertThat(logReplicator.getMatchIndex()).isEqualTo(lastAppendedIndex);
        }

        @Test
        void willOnlyAdvanceIndices() {
            int lastAppendedIndex = INITIAL_NEXT_INDEX + 1;
            logReplicator.logSuccessResponse(lastAppendedIndex);
            assertThat(logReplicator.getMatchIndex()).isEqualTo(lastAppendedIndex);
            assertThat(logReplicator.getNextIndex()).isEqualTo(lastAppendedIndex + 1);

            logReplicator.logSuccessResponse(lastAppendedIndex - 1);
            assertThat(logReplicator.getMatchIndex()).isEqualTo(lastAppendedIndex);
            assertThat(logReplicator.getNextIndex()).isEqualTo(lastAppendedIndex + 1);
        }
    }

    @Nested
    class LogFailedResponse {

        @Test
        void willDecrementNextIndexWhenFollowerLastLogIndexNotProvided() {
            logReplicator.logFailedResponse(null);
            assertThat(logReplicator.getNextIndex()).isEqualTo(INITIAL_NEXT_INDEX - 1);
        }

        @Test
        void willReduceNextIndexToFollowerLastIndexPlusOneWhenFollowerLastIndexProvided() {
            logReplicator.logFailedResponse(2);
            assertThat(logReplicator.getNextIndex()).isEqualTo(3);
        }

        @Test
        void willDecrementNextIndexWhenFollowerLastLogIndexIsAfterNextIndex() {
            logReplicator.logFailedResponse(20);
            assertThat(logReplicator.getNextIndex()).isEqualTo(INITIAL_NEXT_INDEX -1);
        }

        @Test
        void willSetNextIndexToOneIfFollowerLastLogIndexIsZero() {
            logReplicator.logFailedResponse(0);
            assertThat(logReplicator.getNextIndex()).isEqualTo(1);
        }

        @Test
        void willNotModifyMatchIndex() {
            logReplicator.logFailedResponse(null);
            assertThat(logReplicator.getMatchIndex()).isZero();
        }
    }

    @Nested
    class Replicate {

        @Test
        void willSendEmptyAppendEntriesRequest_WhenThereAreNoLogEntries() {
            logReplicator = new LogReplicator<>(logContaining(), CURRENT_TERM, cluster, FOLLOWER_ID, MAX_BATCH_SIZE, 1, new SynchronousReplicationScheduler());
            logReplicator.replicate();
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, 0, Optional.empty(), emptyList(), 0);
        }

        @Test
        void shouldSendEmptyAppendEntriesRequest_WhenFollowerIsCaughtUp() {
            logReplicator.replicate();
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
        }

        @Test
        void shouldSendUpToMaxBatchSizeEntries_WhenFollowerIsLagging() {
            int maxBatchSize = 2;
            logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, FOLLOWER_ID, maxBatchSize, LAST_LOG_INDEX - 2, new SynchronousReplicationScheduler());
            logReplicator.replicate();
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX - 3, Optional.of(ENTRY_ONE.getTerm()), List.of(ENTRY_TWO, ENTRY_THREE), COMMIT_INDEX);
        }
    }
}