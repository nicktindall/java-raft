package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistry;
import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.ReplicationManager;
import au.id.tindall.distalg.raft.replication.ReplicationManagerFactory;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManager;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManagerFactory;
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
    @Mock
    private Configuration<Long> configuration;
    @Mock
    private ClusterMembershipChangeManagerFactory<Long> clusterMembershipChangeManagerFactory;

    @Mock
    private ClusterMembershipChangeManager<Long> clusterMembershipChangeManager;

    @BeforeEach
    void setUp() {
        serverStateFactory = new ServerStateFactory<>(persistentState, log, cluster, configuration, pendingResponseRegistryFactory, clientSessionStore, commandExecutor, electionScheduler, leadershipTransferFactory,
                replicationManagerFactory, clusterMembershipChangeManagerFactory);
    }

    @Test
    void willCreateLeaderState() {
        when(pendingResponseRegistryFactory.createPendingResponseRegistry(clientSessionStore, commandExecutor)).thenReturn(pendingResponseRegistry);
        when(leadershipTransferFactory.create(replicationManager)).thenReturn(leadershipTransfer);
        when(replicationManagerFactory.createReplicationManager()).thenReturn(replicationManager);
        when(clusterMembershipChangeManagerFactory.createChangeManager(replicationManager)).thenReturn(clusterMembershipChangeManager);

        assertThat(serverStateFactory.createLeader())
                .usingRecursiveComparison().isEqualTo(new Leader<>(persistentState, log, cluster, pendingResponseRegistry, serverStateFactory, replicationManager, clientSessionStore, leadershipTransfer, clusterMembershipChangeManager));
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