package au.id.tindall.distalg.raft.serverstates.leadershiptransfer;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.replication.LogReplicator;
import au.id.tindall.distalg.raft.state.PersistentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
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
    private LogReplicator<Long> leadingReplicator;
    @Mock
    private LogReplicator<Long> laggingReplicator;
    @Mock
    private LogStorage logStorage;

    private LeadershipTransfer<Long> leadershipTransfer;
    private long currentTimeMillis;

    @BeforeEach
    void setUp() {
        Map<Long, LogReplicator<Long>> replicators = Map.of(
                LEADING_REPLICATOR_ID, leadingReplicator,
                LAGGING_REPLICATOR_ID, laggingReplicator
        );
        leadershipTransfer = new LeadershipTransfer<>(cluster, persistentState, replicators, this::currentTimeMillis);

        lenient().when(leadingReplicator.getMatchIndex()).thenReturn(LAST_LOG_INDEX - 1);
        lenient().when(laggingReplicator.getMatchIndex()).thenReturn(LAST_LOG_INDEX - 2);

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
            when(leadingReplicator.getMatchIndex()).thenReturn(LAST_LOG_INDEX);

            leadershipTransfer.start();

            assertThat(leadershipTransfer.isInProgress()).isTrue();
            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LEADING_REPLICATOR_ID);
        }

        @Test
        void shouldBeIdempotent() {
            when(leadingReplicator.getMatchIndex()).thenReturn(LAST_LOG_INDEX);

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

            verifyNoInteractions(cluster);

            when(leadingReplicator.getMatchIndex()).thenReturn(LAST_LOG_INDEX);

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
        }

        @Test
        void shouldNotSendTimeoutNowRequestIfTargetIsNotUpToDate() {
            leadershipTransfer.start();
            verifyNoInteractions(cluster);

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verifyNoInteractions(cluster);
        }

        @Test
        void shouldAbortTransferIfTransferHasTimedOut() {
            leadershipTransfer.start();
            assertThat(leadershipTransfer.isInProgress()).isTrue();

            currentTimeMillis += 5100;

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verifyNoInteractions(cluster);
            assertThat(leadershipTransfer.isInProgress()).isFalse();
        }

        @Test
        void shouldSelectNextMostUpToDateTargetIfTheCurrentOneHasNotCaughtUp() {
            leadershipTransfer.start();
            assertThat(leadershipTransfer.isInProgress()).isTrue();

            currentTimeMillis += 1100;

            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verifyNoInteractions(cluster);
            assertThat(leadershipTransfer.isInProgress()).isTrue();

            when(laggingReplicator.getMatchIndex()).thenReturn(LAST_LOG_INDEX);
            leadershipTransfer.sendTimeoutNowRequestIfReadyToTransfer();
            verify(cluster).sendTimeoutNowRequest(CURRENT_TERM, LAGGING_REPLICATOR_ID);
        }

        @Test
        void shouldNotSendMultipleTimeoutNowRequestsInsideTheMinimalInterval() {
            leadershipTransfer.start();
            verifyNoInteractions(cluster);

            when(leadingReplicator.getMatchIndex()).thenReturn(LAST_LOG_INDEX);

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
