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

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SKIPPED;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SUCCESS;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SWITCH_TO_LOG_REPLICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
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
            assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
            verifyNoInteractions(cluster);
        }

        @Nested
        class WhenThereIsACurrentSnapshot {

            private static final int LAST_INDEX = 9876;
            private static final int NEW_LAST_INDEX = 13579;

            private Snapshot currentSnapshot;

            @BeforeEach
            void setUp() throws IOException {
                currentSnapshot = createRandomSnapshot(LAST_INDEX);
                when(persistentState.getCurrentSnapshot()).thenReturn(Optional.of(currentSnapshot));
            }

            @Test
            void theFirstChunkIsSentOnInitialSend() {
                assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 0, Arrays.copyOf(snapshotBytes, SNAPSHOT_CHUNK_LENGTH), false);
            }

            @Test
            void theNextChunkIsSentAfterReceiptIsAcknowledged() {
                snapshotReplicator.sendNextReplicationMessage(false);
                reset(cluster);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), 350);
                assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 351, Arrays.copyOfRange(snapshotBytes, 351, SNAPSHOT_CHUNK_LENGTH + 351), false);
            }

            @Test
            void indicatesWhenTheLastChunkIsBeingSent() {
                snapshotReplicator.sendNextReplicationMessage(false);
                reset(cluster);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), 8192);
                assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 8193, Arrays.copyOfRange(snapshotBytes, 8193, snapshotBytes.length), true);
            }

            @Test
            void transitionsToLogReplicationAfterTheLastByteIsAcknowledged() {
                snapshotReplicator.sendNextReplicationMessage(false);
                reset(cluster);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), SNAPSHOT_LENGTH_BYTES);
                assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SWITCH_TO_LOG_REPLICATION);
                verifyNoInteractions(cluster);
            }

            @Test
            void beginsSendingNewSnapshotFromStartWhenSnapshotChanges() throws IOException {
                snapshotReplicator.sendNextReplicationMessage(false);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 0, Arrays.copyOf(snapshotBytes, SNAPSHOT_CHUNK_LENGTH), false);
                snapshotReplicator.logSuccessSnapshotResponse(currentSnapshot.getLastIndex(), 4096);
                currentSnapshot = createRandomSnapshot(NEW_LAST_INDEX); // create new snapshot
                when(persistentState.getCurrentSnapshot()).thenReturn(Optional.of(currentSnapshot));
                assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                        null, 0, 0, Arrays.copyOf(snapshotBytes, SNAPSHOT_CHUNK_LENGTH), false);
            }

            @Test
            void willNotReSendSameChunkTwiceWhenNotForced() {
                assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
                assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SKIPPED);
                verify(cluster).sendInstallSnapshotRequest(any(), any(), anyInt(), any(), any(), anyInt(), anyInt(), any(), anyBoolean());
            }

            @Test
            void willReSendSameChunkTwiceWhenForced() {
                assertThat(snapshotReplicator.sendNextReplicationMessage(true)).isEqualTo(SUCCESS);
                assertThat(snapshotReplicator.sendNextReplicationMessage(true)).isEqualTo(SUCCESS);
                verify(cluster, times(2)).sendInstallSnapshotRequest(any(), any(), anyInt(), any(), any(), anyInt(), anyInt(), any(), anyBoolean());
            }
        }
    }

    @Nested
    class LogSuccessResponseSnapshot {

        private static final int LAST_INDEX = 9876;
        private Snapshot currentSnapshot;

        @BeforeEach
        void setUp() throws IOException {
            currentSnapshot = createRandomSnapshot(LAST_INDEX);
            when(persistentState.getCurrentSnapshot()).thenReturn(Optional.of(currentSnapshot));
        }

        @Test
        void ignoresResponsesForNonCurrentSnapshots() {
            snapshotReplicator.sendNextReplicationMessage(false);
            reset(cluster);
            snapshotReplicator.logSuccessSnapshotResponse(LAST_INDEX, 350);
            snapshotReplicator.logSuccessSnapshotResponse(LAST_INDEX - 100, 3500); // this is ignored because lastIndex is not current
            assertThat(snapshotReplicator.sendNextReplicationMessage(false)).isEqualTo(SUCCESS);
            verify(cluster).sendInstallSnapshotRequest(CURRENT_TERM, FOLLOWER_ID, currentSnapshot.getLastIndex(), currentSnapshot.getLastTerm(),
                    null, 0, 351, Arrays.copyOfRange(snapshotBytes, 351, SNAPSHOT_CHUNK_LENGTH + 351), false);
        }
    }

    private Snapshot createRandomSnapshot(int lastIndex) throws IOException {
        Snapshot snapshot = new InMemorySnapshot(lastIndex, new Term(4), null);
        snapshot.finaliseSessions();
        snapshotBytes = new byte[SNAPSHOT_LENGTH_BYTES];
        ThreadLocalRandom.current().nextBytes(snapshotBytes);
        snapshot.writeBytes(0, snapshotBytes);
        snapshot.finalise();
        return snapshot;
    }
}