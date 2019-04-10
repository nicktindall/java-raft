package au.id.tindall.distalg.raft.serverstates;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import au.id.tindall.distalg.raft.client.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ServerStateFactoryTest {

    private static final long SERVER_ID = 123L;
    private static final Term TERM = new Term(4);
    private static final long VOTED_FOR = 890;
    private ServerStateFactory<Long> serverStateFactory;

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private Log log;
    @Mock
    private PendingResponseRegistryFactory pendingResponseRegistryFactory;
    @Mock
    private LogReplicatorFactory<Long> logReplicatorFactory;
    @Mock
    private PendingResponseRegistry pendingResponseRegistry;

    @BeforeEach
    void setUp() {
        serverStateFactory = new ServerStateFactory<>(SERVER_ID, log, cluster, pendingResponseRegistryFactory, logReplicatorFactory);
    }

    @Test
    void willCreateLeaderState() {
        when(pendingResponseRegistryFactory.createPendingResponseRegistry()).thenReturn(pendingResponseRegistry);

        assertThat(serverStateFactory.createLeader(TERM))
                .isEqualToComparingFieldByFieldRecursively(new Leader<>(TERM, log, cluster, pendingResponseRegistryFactory, logReplicatorFactory, serverStateFactory));
    }

    @Test
    void willCreateFollowerStateWithEmptyVotedFor() {
        assertThat(serverStateFactory.createFollower(TERM))
                .isEqualToComparingFieldByFieldRecursively(new Follower<>(TERM, null, log, cluster, serverStateFactory));
    }

    @Test
    void willCreateFollowerState() {
        assertThat(serverStateFactory.createFollower(TERM, VOTED_FOR))
                .isEqualToComparingFieldByFieldRecursively(new Follower<>(TERM, VOTED_FOR, log, cluster, serverStateFactory));
    }

    @Test
    void willCreateCandidateState() {
        assertThat(serverStateFactory.createCandidate(TERM))
                .isEqualToComparingFieldByFieldRecursively(new Candidate<>(TERM, log, cluster, SERVER_ID, serverStateFactory));
    }

}