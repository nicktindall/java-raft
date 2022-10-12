package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.cluster.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReplicationManagerTest {

    public static final int LOCAL_ID = 99;
    @Mock
    private SingleClientReplicator<Integer> logReplicatorOne;
    @Mock
    private SingleClientReplicator<Integer> logReplicatorTwo;
    @Mock
    private SingleClientReplicator<Integer> logReplicatorThree;
    @Mock
    private SingleClientReplicatorFactory<Integer> singleClientReplicatorFactory;
    @Mock
    private Configuration<Integer> configuration;

    private ReplicationManager<Integer> replicationManager;

    @BeforeEach
    void setUp() {
        when(configuration.getLocalId()).thenReturn(LOCAL_ID);
        when(configuration.getOtherServerIds()).thenReturn(Set.of(1, 2));
        replicationManager = new ReplicationManager<>(configuration, singleClientReplicatorFactory);
        when(singleClientReplicatorFactory.createReplicator(LOCAL_ID, 1, replicationManager)).thenReturn(logReplicatorOne);
        when(singleClientReplicatorFactory.createReplicator(LOCAL_ID, 2, replicationManager)).thenReturn(logReplicatorTwo);
    }

    @Nested
    class Start {

        @Test
        void willCreateAndStartReplicatorsForAllFollowers() {
            replicationManager.start();
            verify(singleClientReplicatorFactory).createReplicator(LOCAL_ID, 1, replicationManager);
            verify(singleClientReplicatorFactory).createReplicator(LOCAL_ID, 2, replicationManager);
            verify(logReplicatorOne).start();
            verify(logReplicatorTwo).start();
        }
    }

    @Nested
    class Stop {

        @BeforeEach
        void setUp() {
            replicationManager.start();
        }

        @Test
        void willStopAllReplicators() {
            replicationManager.stop();
            verify(logReplicatorOne).stop();
            verify(logReplicatorTwo).stop();
        }
    }

    @Nested
    class StartReplicatingTo {

        @BeforeEach
        void setUp() {
            replicationManager.start();
            when(singleClientReplicatorFactory.createReplicator(LOCAL_ID, 3, replicationManager)).thenReturn(logReplicatorThree);
        }

        @Test
        void willCreateANewReplicatorAndStartReplicatingToIt() {
            replicationManager.startReplicatingTo(3);

            verify(singleClientReplicatorFactory).createReplicator(LOCAL_ID, 3, replicationManager);
            final InOrder sequence = inOrder(logReplicatorThree);
            sequence.verify(logReplicatorThree).start();
            sequence.verify(logReplicatorThree).replicate();
        }
    }

    @Nested
    class StopReplicatingTo {

        @BeforeEach
        void setUp() {
            replicationManager.start();
        }

        @Test
        void willStopAndRemoveReplicatorForPeer() {
            replicationManager.stopReplicatingTo(2);

            logReplicatorTwo.stop();
        }

        @Test
        void willDoNothingWhenIdIsNotPresent() {
            replicationManager.stopReplicatingTo(LOCAL_ID);
        }
    }


    @Nested
    class Replicate {

        @BeforeEach
        void setUp() {
            replicationManager.start();
        }

        @Test
        void WillReplicateToASingleFollower() {
            replicationManager.replicate(1);

            verify(logReplicatorOne).replicate();
            verify(logReplicatorTwo, never()).replicate();
        }

        @Test
        void willReplicateToAllFollowers() {
            replicationManager.replicate();

            verify(logReplicatorOne).replicate();
            verify(logReplicatorTwo).replicate();
        }
    }

    @Nested
    class GetFollowerMatchIndices {

        @BeforeEach
        void setUp() {
            replicationManager.start();
        }

        @Test
        void willReturnMatchIndicesForAllFollowers() {
            when(logReplicatorOne.getMatchIndex()).thenReturn(123);
            when(logReplicatorTwo.getMatchIndex()).thenReturn(456);

            assertThat(replicationManager.getFollowerMatchIndices())
                    .containsExactlyInAnyOrder(123, 456);
        }
    }

    @Nested
    class ReplicatorDelegations {

        @BeforeEach
        void setUp() {
            replicationManager.start();
        }

        @Test
        void willGetMatchIndex() {
            when(logReplicatorOne.getMatchIndex()).thenReturn(123);

            assertThat(replicationManager.getMatchIndex(1)).isEqualTo(123);
        }

        @Test
        void willGetNextIndex() {
            when(logReplicatorOne.getNextIndex()).thenReturn(123);

            assertThat(replicationManager.getNextIndex(1)).isEqualTo(123);
        }

        @Test
        void willLogSuccessResponse() {
            replicationManager.logSuccessResponse(1, 123);

            verify(logReplicatorOne).logSuccessResponse(123);
        }

        @Test
        void willNotFailWhenLoggingSuccessResponseForMissingFollower() {
            replicationManager.stopReplicatingTo(1);
            replicationManager.logSuccessResponse(1, 123);
        }

        @Test
        void willLogFailedResponse() {
            replicationManager.logFailedResponse(1, 123);

            verify(logReplicatorOne).logFailedResponse(123);
        }

        @Test
        void willNotFailWhenLoggingFailedResponseForMissingFollower() {
            replicationManager.stopReplicatingTo(1);
            replicationManager.logFailedResponse(1, 123);
        }
    }

    @Nested
    class ReplicateIfTrailing {

        @BeforeEach
        void setUp() {
            replicationManager.start();
        }

        @Test
        void willReplicateWhenNextIndexIsLessThanOrEqualToLastLogIndex() {
            when(logReplicatorOne.getNextIndex()).thenReturn(1);
            replicationManager.replicateIfTrailingIndex(1, 1);
            replicationManager.replicateIfTrailingIndex(1, 2);
            verify(logReplicatorOne, times(2)).replicate();
        }

        @Test
        void willNotReplicateWhenNextIndexIsGreaterThanLastLogIndex() {
            when(logReplicatorOne.getNextIndex()).thenReturn(3);
            replicationManager.replicateIfTrailingIndex(1, 2);
            verify(logReplicatorOne, never()).replicate();
        }

        @Test
        void willDoNothingWhenIdIsNotPresent() {
            replicationManager.replicateIfTrailingIndex(LOCAL_ID, 2);
        }
    }
}