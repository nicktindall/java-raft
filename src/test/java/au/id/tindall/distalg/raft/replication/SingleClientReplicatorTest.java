package au.id.tindall.distalg.raft.replication;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.function.Supplier;

import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.COULD_NOT_REPLICATE;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SKIPPED;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SUCCESS;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SWITCH_TO_LOG_REPLICATION;
import static au.id.tindall.distalg.raft.replication.StateReplicator.ReplicationResult.SWITCH_TO_SNAPSHOT_REPLICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SingleClientReplicatorTest {

    @Mock
    private ReplicationScheduler replicationScheduler;
    @Mock
    private LogReplicatorFactory<Integer> logReplicatorFactory;
    @Mock
    private LogReplicator<Integer> logReplicator;
    @Mock
    private SnapshotReplicatorFactory<Integer> snapshotReplicatorFactory;
    @Mock
    private SnapshotReplicator<Integer> snapshotReplicator;
    @Mock
    private ReplicationState<Integer> replicationState;

    private Supplier<Boolean> heartbeatReplication;

    private SingleClientReplicator<Integer> replicator;

    @BeforeEach
    void setUp() {
        lenient().when(snapshotReplicatorFactory.createSnapshotReplicator(replicationState)).thenReturn(snapshotReplicator);
        lenient().when(logReplicatorFactory.createLogReplicator(replicationState)).thenReturn(logReplicator);
        doAnswer(iom -> {
            heartbeatReplication = iom.getArgument(0);
            return null;
        }).when(replicationScheduler).setSendAppendEntriesRequest(any());
        replicator = new SingleClientReplicator<>(replicationScheduler, logReplicatorFactory, snapshotReplicatorFactory, replicationState);
    }

    @Nested
    class Constructor {

        @Test
        void willPopulateHeartbeatMethod() {
            verify(replicationScheduler).setSendAppendEntriesRequest(any());
        }

        @Test
        void willInitialiseLogReplicator() {
            verify(logReplicatorFactory).createLogReplicator(replicationState);
            when(logReplicator.sendNextReplicationMessage(false)).thenReturn(StateReplicator.ReplicationResult.SUCCESS);
            replicator.replicate();
            verify(logReplicator).sendNextReplicationMessage(false);
        }
    }


    @Test
    void startWillStartScheduler() {
        replicator.start();
        verify(replicationScheduler).start();
    }

    @Test
    void stopWillStopScheduler() {
        replicator.stop();
        verify(replicationScheduler).stop();
    }

    @Test
    void logSuccessResponseDelegatesToReplicationState() {
        replicator.logSuccessResponse(1234);
        verify(replicationState).logSuccessResponse(1234);
    }

    @Test
    void logFailedResponseDelegatesToReplicationState() {
        replicator.logFailedResponse(1234);
        verify(replicationState).logFailedResponse(1234);
    }

    @Test
    void getMatchIndexWillDelegateToReplicationState() {
        when(replicationState.getMatchIndex()).thenReturn(1234);
        assertThat(replicator.getMatchIndex()).isEqualTo(1234);
    }

    @Test
    void getNextIndexWillDelegateToReplicationState() {
        when(replicationState.getNextIndex()).thenReturn(1234);
        assertThat(replicator.getNextIndex()).isEqualTo(1234);
    }

    @Test
    void logSuccessfulSnapshotResponseWillDelegateToReplicator() {
        replicator.logSuccessSnapshotResponse(1234, 5678);
        verify(logReplicator).logSuccessSnapshotResponse(1234, 5678);
    }

    @Nested
    class Replicate {

        @Test
        void willSwitchToSnapshotReplication() {
            when(logReplicator.sendNextReplicationMessage(false)).thenReturn(SWITCH_TO_SNAPSHOT_REPLICATION);
            when(snapshotReplicator.sendNextReplicationMessage(false)).thenReturn(SUCCESS);
            replicator.replicate();
            verify(snapshotReplicator).sendNextReplicationMessage(false);
        }

        @Test
        void willSwitchBackToLogReplication() {
            when(logReplicator.sendNextReplicationMessage(false)).thenReturn(SWITCH_TO_SNAPSHOT_REPLICATION, SUCCESS);
            when(snapshotReplicator.sendNextReplicationMessage(false)).thenReturn(SWITCH_TO_LOG_REPLICATION);
            replicator.replicate();
            verify(snapshotReplicator).sendNextReplicationMessage(false);
            verify(logReplicator, times(2)).sendNextReplicationMessage(false);
        }

        @Test
        void willDoNothingWhenSkipped() {
            when(logReplicator.sendNextReplicationMessage(false)).thenReturn(SKIPPED);
            replicator.replicate();
            verify(logReplicator).sendNextReplicationMessage(anyBoolean());
            verify(snapshotReplicator, never()).sendNextReplicationMessage(anyBoolean());
        }

        @Test
        void willDoNothingWhenCouldNotReplicate() {
            when(logReplicator.sendNextReplicationMessage(false)).thenReturn(COULD_NOT_REPLICATE);
            replicator.replicate();
            verify(logReplicator).sendNextReplicationMessage(anyBoolean());
            verify(snapshotReplicator, never()).sendNextReplicationMessage(anyBoolean());
        }
    }

    @Nested
    class HeartbeatReplication {

        @Test
        void willForceReplication() {
            when(logReplicator.sendNextReplicationMessage(anyBoolean())).thenReturn(SUCCESS);
            assertThat(heartbeatReplication.get()).isTrue();
            verify(logReplicator).sendNextReplicationMessage(true);
        }
    }
}