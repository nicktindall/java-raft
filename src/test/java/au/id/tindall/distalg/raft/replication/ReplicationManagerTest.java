package au.id.tindall.distalg.raft.replication;

import au.id.tindall.distalg.raft.comms.Cluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReplicationManagerTest {

    @Mock
    private LogReplicator<Integer> logReplicatorOne;
    @Mock
    private LogReplicator<Integer> logReplicatorTwo;
    @Mock
    private Cluster<Integer> cluster;
    @Mock
    private LogReplicatorFactory<Integer> logReplicatorFactory;

    private ReplicationManager<Integer> replicationManager;

    @BeforeEach
    void setUp() {
        when(cluster.getOtherMemberIds()).thenReturn(Set.of(1, 2));
        when(logReplicatorFactory.createLogReplicator(1)).thenReturn(logReplicatorOne);
        when(logReplicatorFactory.createLogReplicator(2)).thenReturn(logReplicatorTwo);
        replicationManager = new ReplicationManager<>(cluster, logReplicatorFactory);
    }

    @Nested
    class Start {

        @Test
        void willCreateAndStartReplicatorsForAllFollowers() {
            replicationManager.start();
            verify(logReplicatorFactory).createLogReplicator(1);
            verify(logReplicatorFactory).createLogReplicator(2);
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
        void willLogFailedResponse() {
            replicationManager.logFailedResponse(1);

            verify(logReplicatorOne).logFailedResponse();
        }
    }
}