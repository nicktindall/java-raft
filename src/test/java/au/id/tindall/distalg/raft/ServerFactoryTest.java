package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.cluster.Configuration;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.comms.Inbox;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.log.storage.LogStorage;
import au.id.tindall.distalg.raft.processors.ProcessorManager;
import au.id.tindall.distalg.raft.processors.ProcessorManagerFactory;
import au.id.tindall.distalg.raft.processors.RaftProcessorGroup;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.replication.ReplicationManagerFactory;
import au.id.tindall.distalg.raft.replication.ReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.replication.ReplicationStateFactory;
import au.id.tindall.distalg.raft.replication.SingleClientReplicatorFactory;
import au.id.tindall.distalg.raft.replication.SnapshotReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManagerFactory;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.snapshotting.SnapshotHeuristic;
import au.id.tindall.distalg.raft.snapshotting.Snapshotter;
import au.id.tindall.distalg.raft.snapshotting.SnapshotterFactory;
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

import java.io.Serializable;
import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
    private ElectionScheduler electionScheduler;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private LogStorage logStorage;
    @Mock
    private ReplicationSchedulerFactory<Long> replicationSchedulerFactory;
    @Mock
    private SnapshotterFactory snapshotterFactory;
    @Mock
    private Snapshotter snapshotter;
    @Mock
    private SnapshotHeuristic snapshotHeuristic;
    @Mock
    private ProcessorManager<RaftProcessorGroup> processorManager;
    @Mock
    private ProcessorManagerFactory processorManagerFactory;
    @Mock
    private InboxFactory<Long> inboxFactory;
    @Mock
    private Inbox<Long> inbox;
    private ServerFactory<Long> serverFactory;

    @BeforeEach
    void setUp() {
        when(inboxFactory.createInbox(SERVER_ID)).thenReturn(inbox);
        when(persistentState.getId()).thenReturn(SERVER_ID);
        when(persistentState.getLogStorage()).thenReturn(logStorage);
        when(clientSessionStoreFactory.create(MAX_CLIENT_SESSIONS)).thenReturn(clientSessionStore);
        when(clusterFactory.createCluster(SERVER_ID)).thenReturn(cluster);
        when(logFactory.createLog(logStorage)).thenReturn(log);
        when(stateMachineFactory.createStateMachine()).thenReturn(stateMachine);
        when(electionSchedulerFactory.createElectionScheduler(SERVER_ID)).thenReturn(electionScheduler);
        when(commandExecutorFactory.createCommandExecutor(stateMachine, clientSessionStore, snapshotter)).thenReturn(commandExecutor);
        when(snapshotterFactory.create(eq(log), eq(clientSessionStore), eq(stateMachine), eq(persistentState), any(SnapshotHeuristic.class))).thenReturn(snapshotter);
        when(processorManagerFactory.create(any(Serializable.class))).thenReturn(processorManager);
        serverFactory = new ServerFactory<>(clusterFactory, logFactory, pendingResponseRegistryFactory, clientSessionStoreFactory, MAX_CLIENT_SESSIONS,
                commandExecutorFactory, stateMachineFactory, electionSchedulerFactory, MAX_BATCH_SIZE, replicationSchedulerFactory, ELECTION_TIMEOUT, snapshotterFactory, false, processorManagerFactory, inboxFactory);
    }

    @Test
    void createsServersAndTheirDependencies() {
        final Configuration<Long> configuration = new Configuration<>(SERVER_ID, Set.of(SERVER_ID, OTHER_SERVER_ID), ELECTION_TIMEOUT);
        assertThat(serverFactory.create(persistentState, Set.of(SERVER_ID, OTHER_SERVER_ID), snapshotHeuristic)).usingRecursiveComparison().isEqualTo(
                new ServerImpl<>(
                        persistentState,
                        new ServerStateFactory<>(
                                persistentState,
                                log,
                                cluster,
                                pendingResponseRegistryFactory,
                                clientSessionStore,
                                commandExecutor,
                                electionScheduler,
                                new LeadershipTransferFactory<>(cluster, persistentState, configuration),
                                new ReplicationManagerFactory<>(configuration,
                                        new SingleClientReplicatorFactory<>(replicationSchedulerFactory,
                                                new LogReplicatorFactory<>(log, persistentState, cluster, MAX_BATCH_SIZE),
                                                new SnapshotReplicatorFactory<>(persistentState, cluster),
                                                new ReplicationStateFactory<>(log))
                                ),
                                new ClusterMembershipChangeManagerFactory<>(log, persistentState, configuration),
                                false,
                                processorManager,
                                configuration
                        ),
                        stateMachine,
                        cluster,
                        electionScheduler,
                        processorManager,
                        inbox)
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