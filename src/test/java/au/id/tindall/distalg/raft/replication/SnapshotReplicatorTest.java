package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.state.InMemorySnapshot;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.state.Snapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.StayInCurrentMode;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SwitchToLogReplication;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnapshotReplicatorTest {

    private static final Term CURRENT_TERM = Term.ZERO;
    private static final int FOLLOWER_ID = 12345;
    private static final int SNAPSHOT_CHUNK_LENGTH = 4096;
    private static final int SNAPSHOT_LENGTH_BYTES = 10_240;

    private byte[] snapshotBytes;

    @Mock
    private Cluster<Integer> cluster;
    @Mock
    private PersistentState<Integer> persistentState;
    @Mock
    private ReplicationState<Integer> replicationState;

    private SnapshotReplicator<Integer> snapshotReplicator;

    @BeforeEach
    void setUp() {
        lenient().when(replicationState.getFollowerId()).thenReturn(FOLLOWER_ID);
        snapshotReplicator = new SnapshotReplicator<>(CURRENT_TERM, cluster, persistentState, replicationState);
    }

    @Nested
    class SendNextReplicationMessage {

        @Test
        void willDoNothingAndStayInCurrentStateWhenThereIsNoCurrentSnapshot() {
            when(persistentState.getCurrentSnapshot()).thenReturn(Optional.empty());
            assertThat(snapshotReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
            verifyNoInteractions(cluster);
        }

        @Nested
        class WhenThereIsACurrentSnapshot {

            private static final int LAST_INDEX = 9876;
            private static final int NEW_LAST_INDEX = 13579;

            private Snapshot currentSnapshot;

            @BeforeEach
            void setUp() {
                currentSnapshot = createRandomSnapshot(LAST_INDEX);
                when(persistentState.getCurrentSnapshot()).thenReturn(Optional.of(currentSnapshot));
            }

            @Test
            void theFirstChunkIsSentOnInitialSend() {
                assertThat(snapshotReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 0, Arrays.copyOf(snapshotBytes, SNAPSHOT_CHUNK_LENGTH), false);
            }

            @Test
            void theNextChunkIsSentAfterReceiptIsAcknowledged() {
                snapshotReplicator.sendNextReplicationMessage();
                reset(cluster);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), 350);
                assertThat(snapshotReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 351, Arrays.copyOfRange(snapshotBytes, 351, SNAPSHOT_CHUNK_LENGTH + 351), false);
            }

            @Test
            void indicatesWhenTheLastChunkIsBeingSent() {
                snapshotReplicator.sendNextReplicationMessage();
                reset(cluster);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), 8192);
                assertThat(snapshotReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 8193, Arrays.copyOfRange(snapshotBytes, 8193, snapshotBytes.length), true);
            }

            @Test
            void transitionsToLogReplicationAfterTheLastByteIsAcknowledged() {
                snapshotReplicator.sendNextReplicationMessage();
                reset(cluster);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), SNAPSHOT_LENGTH_BYTES);
                assertThat(snapshotReplicator.sendNextReplicationMessage()).isEqualTo(SwitchToLogReplication);
                verifyNoInteractions(cluster);
            }

            @Test
            void beginsSendingNewSnapshotFromStartWhenSnapshotChanges() {
                snapshotReplicator.sendNextReplicationMessage();
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 0, Arrays.copyOf(snapshotBytes, SNAPSHOT_CHUNK_LENGTH), false);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), 4096);
                currentSnapshot = createRandomSnapshot(NEW_LAST_INDEX); // create new snapshot
                when(persistentState.getCurrentSnapshot()).thenReturn(Optional.of(currentSnapshot));
                assertThat(snapshotReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 0, Arrays.copyOf(snapshotBytes, SNAPSHOT_CHUNK_LENGTH), false);
            }
        }
    }

    @Nested
    class LogSuccessResponseSnapshot {

        private static final int LAST_INDEX = 9876;
        private Snapshot currentSnapshot;

        @BeforeEach
        void setUp() {
            currentSnapshot = createRandomSnapshot(LAST_INDEX);
            when(persistentState.getCurrentSnapshot()).thenReturn(Optional.of(currentSnapshot));
        }

        @Test
        void ignoresResponsesForNonCurrentSnapshots() {
            snapshotReplicator.sendNextReplicationMessage();
            reset(cluster);
            snapshotReplicator.logSuccessSnapshotResponse(LAST_INDEX, 350);
            snapshotReplicator.logSuccessSnapshotResponse(LAST_INDEX - 100, 3500); // this is ignored because lastIndex is not current
            assertThat(snapshotReplicator.sendNextReplicationMessage()).isEqualTo(StayInCurrentMode);
            verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                    null, 0, 351, Arrays.copyOfRange(snapshotBytes, 351, SNAPSHOT_CHUNK_LENGTH + 351), false);
        }
    }

    private Snapshot createRandomSnapshot(int lastIndex) {
        Snapshot snapshot = new InMemorySnapshot(lastIndex, new Term(4), null);
        snapshot.finaliseSessions();
        snapshotBytes = new byte[SNAPSHOT_LENGTH_BYTES];
        ThreadLocalRandom.current().nextBytes(snapshotBytes);
        snapshot.writeBytes(0, snapshotBytes);
        snapshot.finalise();
        return snapshot;
    }
}