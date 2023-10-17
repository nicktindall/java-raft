package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LeadershipTransferTest {

    private static final Long LEADING_REPLICATOR_ID = 111L;
    private static final Long LAGGING_REPLICATOR_ID = 222L;
    private static final Term CURRENT_TERM = new Term(0);
    private static final int LAST_LOG_INDEX = 10;

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private LogStorage logStorage;
    @Mock
    private ReplicationManager<Long> replicationManager;
    @Mock
    private Configuration<Long> configuration;

    private LeadershipTransfer<Long> leadershipTransfer;
    private long currentTimeMillis;

    @BeforeEach
    void setUp() {
        leadershipTransfer = new LeadershipTransfer<>(cluster, persistentState, replicationManager, configuration, this::currentTimeMillis);

        lenient().when(configuration.getOtherServerIds()).thenReturn(Set.of(LEADING_REPLICATOR_ID, LAGGING_REPLICATOR_ID));
        lenient().when(replicationManager.getMatchIndex(LEADING_REPLICATOR_ID)).thenReturn(LAST_LOG_INDEX - 1);
        lenient().when(replicationManager.getMatchIndex(LAGGING_REPLICATOR_ID)).thenReturn(LAST_LOG_INDEX - 2);

        lenient().when(logStorage.getLastLogIndex()).thenReturn(LAST_LOG_INDEX);
        lenient().when(persistentState.getLogStorage()).thenReturn(logStorage);
        lenient().when(persistentState.getCurrentTerm()).thenReturn(CURRENT_TERM);

        currentTimeMillis = System.currentTimeMillis();
    }

    private long currentTimeMillis() {
        return this.currentTimeMillis;
    }

    @Nested
    class Start {

        @Test
        void shouldNotSendTimeoutNotRequestIfNoFollowerIsUpToDate() {
            leadershipTransfer.start();

            assertThat(leadershipTransfer.isInProgress()).isTrue();
            verify(cluster, never()).sendTimeoutNowRequest(any(), any());
        }

        @Test
        void shouldSelectMostUpToDatePeerToTransferTo() {
            when(replicationManager.getMatchIndex(LEADING_REPLICATOR_ID)).thenReturn(LAST_LOG_INDEX);

            leadershipTransfer.start();

            assertThat(leadershipTransfer.isInProgress()).isTrue();
            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LEADING_REPLICATOR_ID);
        }

        @Test
        void shouldBeIdempotent() {
            when(replicationManager.getMatchIndex(LEADING_REPLICATOR_ID)).thenReturn(LAST_LOG_INDEX);

            leadershipTransfer.start();
            leadershipTransfer.start();

            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LEADING_REPLICATOR_ID);
        }
    }

    @Nested
    class IsInProgress {

        @Test
        void shouldReturnFalseWhenATransferIsInProgress() {
            assertThat(leadershipTransfer.isInProgress()).isFalse();
        }

        @Test
        void shouldReturnTrueWhenATransferIsInProgress() {
            leadershipTransfer.start();
            assertThat(leadershipTransfer.isInProgress()).isTrue();
        }

        @Test
        void shouldReturnFalseAfterTransferHasTimedOut() {
            leadershipTransfer.start();
            assertThat(leadershipTransfer.isInProgress()).isTrue();
            currentTimeMillis += 5100;
            assertThat(leadershipTransfer.isInProgress()).isFalse();
        }
    }

    @Nested
    class SendTimeoutNowRequestIfReadyToTransfer {

        @Test
        void shouldDoNothingIfNoTransferIsInProgress() {
            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();

            verifyNoInteractions(cluster);
            assertThat(leadershipTransfer.isInProgress()).isFalse();
        }

        @Test
        void shouldSendTimeoutNowRequestIfTargetIsUpToDate() {
            leadershipTransfer.start();

            verify(cluster, never()).sendTimeoutNowRequest(any(), any());
            when(replicationManager.getMatchIndex(LEADING_REPLICATOR_ID)).thenReturn(LAST_LOG_INDEX);

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LEADING_REPLICATOR_ID);
        }

        @Test
        void shouldNotSendTimeoutNowRequestIfTargetIsNotUpToDate() {
            leadershipTransfer.start();
            verify(cluster, never()).sendTimeoutNowRequest(any(), any());

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster, never()).sendTimeoutNowRequest(any(), any());
        }

        @Test
        void shouldAbortTransferIfTransferHasTimedOut() {
            leadershipTransfer.start();
            assertThat(leadershipTransfer.isInProgress()).isTrue();

            currentTimeMillis += 5100;

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();

            verify(cluster, never()).sendTimeoutNowRequest(any(), any());
            assertThat(leadershipTransfer.isInProgress()).isFalse();
        }

        @Test
        void shouldSelectNextMostUpToDateTargetIfTheCurrentOneHasNotCaughtUp() {
            leadershipTransfer.start();
            assertThat(leadershipTransfer.isInProgress()).isTrue();

            currentTimeMillis += 1100;

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster, never()).sendTimeoutNowRequest(any(), any());
            assertThat(leadershipTransfer.isInProgress()).isTrue();

            when(replicationManager.getMatchIndex(LAGGING_REPLICATOR_ID)).thenReturn(LAST_LOG_INDEX);
            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LAGGING_REPLICATOR_ID);
        }

        @Test
        void shouldNotSendMultipleTimeoutNowRequestsInsideTheMinimalInterval() {
            leadershipTransfer.start();
            verify(cluster, never()).sendTimeoutNowRequest(any(), any());

            when(replicationManager.getMatchIndex(LEADING_REPLICATOR_ID)).thenReturn(LAST_LOG_INDEX);

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LEADING_REPLICATOR_ID);

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LEADING_REPLICATOR_ID);

            currentTimeMillis += 110;
            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster, times(2)).sendTimeoutNowRequest(CURRENT_TERM, LEADING_REPLICATOR_ID);
        }
    }
}
