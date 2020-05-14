package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.driver.ElectionScheduler;
import au.id.tindall.distalg.raft.driver.HeartbeatScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerStateFactoryTest {

    private static final long SERVER_ID = 123L;
    private static final long LEADER_ID = 456L;
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
    @Mock
    private ClientSessionStore clientSessionStore;
    @Mock
    private CommandExecutor commandExecutor;
    @Mock
    private ElectionScheduler<Long> electionScheduler;
    @Mock
    private HeartbeatScheduler<Long> heartbeatScheduler;

    @BeforeEach
    void setUp() {
        serverStateFactory = new ServerStateFactory<>(SERVER_ID, log, cluster, pendingResponseRegistryFactory, logReplicatorFactory, clientSessionStore, commandExecutor, electionScheduler, heartbeatScheduler);
    }

    @Test
    void willCreateLeaderState() {
        when(pendingResponseRegistryFactory.createPendingResponseRegistry(clientSessionStore, commandExecutor)).thenReturn(pendingResponseRegistry);

        assertThat(serverStateFactory.createLeader(TERM))
                .isEqualToComparingFieldByFieldRecursively(new Leader<>(TERM, log, cluster, pendingResponseRegistry, logReplicatorFactory, serverStateFactory, clientSessionStore, SERVER_ID));
    }

    @Test
    void willCreateFollowerStateWithEmptyVotedFor() {
        assertThat(serverStateFactory.createFollower(TERM, LEADER_ID))
                .isEqualToComparingFieldByFieldRecursively(new Follower<>(TERM, null, log, cluster, serverStateFactory, LEADER_ID, electionScheduler));
    }

    @Test
    void willCreateFollowerState() {
        assertThat(serverStateFactory.createFollower(TERM, LEADER_ID, VOTED_FOR))
                .isEqualToComparingFieldByFieldRecursively(new Follower<>(TERM, VOTED_FOR, log, cluster, serverStateFactory, LEADER_ID, electionScheduler));
    }

    @Test
    void willCreateCandidateState() {
        assertThat(serverStateFactory.createCandidate(TERM))
                .isEqualToComparingFieldByFieldRecursively(new Candidate<>(TERM, log, cluster, SERVER_ID, serverStateFactory, electionScheduler));
    }

}