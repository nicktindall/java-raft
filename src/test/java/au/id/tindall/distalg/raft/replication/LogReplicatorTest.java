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
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.StayInCurrentMode;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SwitchToSnapshotReplication;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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
    private static final int MAX_BATCH_SIZE = 1;

    private LogReplicator<Long> logReplicator;

    @Mock
    private Cluster<Long> cluster;

    private Log log;

    @BeforeEach
    void setUp() {
        log = logContaining(ENTRY_ONE, ENTRY_TWO, ENTRY_THREE, ENTRY_FOUR);
        log.advanceCommitIndex(COMMIT_INDEX);
        logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, INITIAL_NEXT_INDEX));
    }

    @Nested
    class SendNextReplicationMessage {

        @Test
        void willSendEmptyAppendEntriesRequest_WhenThereAreNoLogEntries() {
            logReplicator = new LogReplicator<>(logContaining(), CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, 1));
            assertThat(logReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, 0, Optional.empty(), emptyList(), 0);
        }

        @Test
        void shouldSendEmptyAppendEntriesRequest_WhenFollowerIsCaughtUp() {
            assertThat(logReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX, Optional.of(ENTRY_FOUR.getTerm()), emptyList(), COMMIT_INDEX);
        }

        @Test
        void shouldSendUpToMaxBatchSizeEntries_WhenFollowerIsLagging() {
            int maxBatchSize = 2;
            logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, maxBatchSize, new ReplicationState<>(FOLLOWER_ID, LAST_LOG_INDEX - 2));
            assertThat(logReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
            verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, LAST_LOG_INDEX - 3, Optional.of(ENTRY_ONE.getTerm()), List.of(ENTRY_TWO, ENTRY_THREE), COMMIT_INDEX);
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
                logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, 2));
                assertThat(logReplicator.sendNextReplicationMessage()).isEqualTo(SwitchToSnapshotReplication);
                verifyNoInteractions(cluster);
            }

            @Test
            void willSendAppendEntriesEventWhenPrevIndexHasBeenTruncated() {
                logReplicator = new LogReplicator<>(log, CURRENT_TERM, cluster, MAX_BATCH_SIZE, new ReplicationState<>(FOLLOWER_ID, 4));
                assertThat(logReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
                verify(cluster).sendAppendEntriesRequest(CURRENT_TERM, FOLLOWER_ID, 3, Optional.of(ENTRY_THREE.getTerm()),
                        List.of(ENTRY_FOUR), COMMIT_INDEX);
            }
        }
    }
}