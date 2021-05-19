package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.replication.ReplicationManagerFactory;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransfer;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
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

    private static final long LEADER_ID = 456L;
    private ServerStateFactory<Long> serverStateFactory;

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private Log log;
    @Mock
    private PendingResponseRegistryFactory pendingResponseRegistryFactory;
    @Mock
    private PendingResponseRegistry pendingResponseRegistry;
    @Mock
    private ClientSessionStore clientSessionStore;
    @Mock
    private CommandExecutor commandExecutor;
    @Mock
    private ElectionScheduler<Long> electionScheduler;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private LeadershipTransferFactory<Long> leadershipTransferFactory;
    @Mock
    private LeadershipTransfer<Long> leadershipTransfer;
    @Mock
    private ReplicationManagerFactory<Long> replicationManagerFactory;
    @Mock
    private ReplicationManager<Long> replicationManager;

    @BeforeEach
    void setUp() {
        serverStateFactory = new ServerStateFactory<>(persistentState, log, cluster, pendingResponseRegistryFactory, clientSessionStore, commandExecutor, electionScheduler, leadershipTransferFactory,
                replicationManagerFactory);
    }

    @Test
    void willCreateLeaderState() {
        when(pendingResponseRegistryFactory.createPendingResponseRegistry(clientSessionStore, commandExecutor)).thenReturn(pendingResponseRegistry);
        when(leadershipTransferFactory.create(replicationManager)).thenReturn(leadershipTransfer);
        when(replicationManagerFactory.createReplicationManager()).thenReturn(replicationManager);

        assertThat(serverStateFactory.createLeader())
                .usingRecursiveComparison().isEqualTo(new Leader<>(persistentState, log, cluster, pendingResponseRegistry, serverStateFactory, replicationManager, clientSessionStore, leadershipTransfer));
    }

    @Test
    void willCreateFollowerState() {
        assertThat(serverStateFactory.createFollower(LEADER_ID))
                .usingRecursiveComparison().isEqualTo(new Follower<>(persistentState, log, cluster, serverStateFactory, LEADER_ID, electionScheduler));
    }

    @Test
    void willCreateCandidateState() {
        assertThat(serverStateFactory.createCandidate())
                .usingRecursiveComparison().isEqualTo(new Candidate<>(persistentState, log, cluster, serverStateFactory, electionScheduler));
    }
}