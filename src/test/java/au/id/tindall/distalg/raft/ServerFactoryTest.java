package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.replication.ReplicationManagerFactory;
import au.id.tindall.distalg.raft.replication.ReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManagerFactory;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import au.id.tindall.distalg.raft.statemachine.StateMachineFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerFactoryTest {

    private static final Long SERVER_ID = 12345L;
    private static final Long OTHER_SERVER_ID = 67890L;
    private static final int MAX_CLIENT_SESSIONS = 999;
    private static final int MAX_BATCH_SIZE = 998;
    private static final Duration ELECTION_TIMEOUT = Duration.ofSeconds(1);

    @Mock
    private ClusterFactory<Long> clusterFactory;
    @Mock
    private Cluster<Long> cluster;
    @Mock
    private LogFactory logFactory;
    @Mock
    private Log log;
    @Mock
    private PendingResponseRegistryFactory pendingResponseRegistryFactory;
    @Mock
    private ClientSessionStoreFactory clientSessionStoreFactory;
    @Mock
    private ClientSessionStore clientSessionStore;
    @Mock
    private CommandExecutorFactory commandExecutorFactory;
    @Mock
    private StateMachineFactory stateMachineFactory;
    @Mock
    private CommandExecutor commandExecutor;
    @Mock
    private StateMachine stateMachine;
    @Mock
    private ElectionSchedulerFactory<Long> electionSchedulerFactory;
    @Mock
    private ElectionScheduler<Long> electionScheduler;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private LogStorage logStorage;
    @Mock
    private ReplicationSchedulerFactory replicationSchedulerFactory;
    private ServerFactory<Long> serverFactory;

    @BeforeEach
    void setUp() {
        when(persistentState.getId()).thenReturn(SERVER_ID);
        when(persistentState.getLogStorage()).thenReturn(logStorage);
        when(clientSessionStoreFactory.create(MAX_CLIENT_SESSIONS)).thenReturn(clientSessionStore);
        when(clusterFactory.createForNode(eq(SERVER_ID))).thenReturn(cluster);
        when(logFactory.createLog(logStorage)).thenReturn(log);
        when(stateMachineFactory.createStateMachine()).thenReturn(stateMachine);
        when(electionSchedulerFactory.createElectionScheduler()).thenReturn(electionScheduler);
        when(commandExecutorFactory.createCommandExecutor(stateMachine, clientSessionStore)).thenReturn(commandExecutor);
        serverFactory = new ServerFactory<>(clusterFactory, logFactory, pendingResponseRegistryFactory, clientSessionStoreFactory, MAX_CLIENT_SESSIONS,
                commandExecutorFactory, stateMachineFactory, electionSchedulerFactory, MAX_BATCH_SIZE, replicationSchedulerFactory, ELECTION_TIMEOUT);
    }

    @Test
    void createsServersAndTheirDependencies() {
        final Configuration<Long> configuration = new Configuration<>(SERVER_ID, Set.of(SERVER_ID, OTHER_SERVER_ID), ELECTION_TIMEOUT);
        assertThat(serverFactory.create(persistentState, Set.of(SERVER_ID, OTHER_SERVER_ID))).usingRecursiveComparison().isEqualTo(
                new Server<>(
                        persistentState,
                        new ServerStateFactory<>(
                                persistentState,
                                log,
                                cluster,
                                configuration,
                                pendingResponseRegistryFactory,
                                clientSessionStore,
                                commandExecutor,
                                electionScheduler,
                                new LeadershipTransferFactory<>(cluster, persistentState),
                                new ReplicationManagerFactory<>(configuration,
                                        new LogReplicatorFactory<>(log, persistentState, cluster, MAX_BATCH_SIZE, replicationSchedulerFactory)
                                ),
                                new ClusterMembershipChangeManagerFactory<>(log, persistentState, configuration)
                        ),
                        stateMachine)
        );
    }

    @Test
    void startsListeningForClientRegistrations() {
        serverFactory.create(persistentState, Set.of(SERVER_ID, OTHER_SERVER_ID));
        verify(clientSessionStore).startListeningForClientRegistrations(log);
    }

    @Test
    void startsListeningForCommittedStateMachineCommands() {
        serverFactory.create(persistentState, Set.of(SERVER_ID, OTHER_SERVER_ID));
        verify(commandExecutor).startListeningForCommittedCommands(log);
    }
}