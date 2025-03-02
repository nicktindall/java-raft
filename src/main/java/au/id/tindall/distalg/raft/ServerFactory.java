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

import java.time.Duration;
import java.util.Set;

import static au.id.tindall.distalg.raft.snapshotting.SnapshotHeuristic.NEVER_SNAPSHOT;

public class ServerFactory<I> {

    private final ClusterFactory<I> clusterFactory;
    private final LogFactory logFactory;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final ClientSessionStoreFactory clientSessionStoreFactory;
    private final int maxClientSessions;
    private final CommandExecutorFactory commandExecutorFactory;
    private final StateMachineFactory stateMachineFactory;
    private final ElectionSchedulerFactory<I> electionSchedulerFactory;
    private final int maxBatchSize;
    private final ReplicationSchedulerFactory<I> replicationSchedulerFactory;
    private final Duration electionTimeout;
    private final SnapshotterFactory snapshotterFactory;
    private final ProcessorManagerFactory processorManagerFactory;
    private final InboxFactory<I> inboxFactory;
    private final boolean timing;

    public ServerFactory(ClusterFactory<I> clusterFactory, LogFactory logFactory, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                         ClientSessionStoreFactory clientSessionStoreFactory, int maxClientSessions,
                         CommandExecutorFactory commandExecutorFactory, StateMachineFactory stateMachineFactory, ElectionSchedulerFactory<I> electionSchedulerFactory,
                         int maxBatchSize, ReplicationSchedulerFactory<I> replicationSchedulerFactory, Duration electionTimeout, SnapshotterFactory snapshotterFactory,
                         boolean timing, ProcessorManagerFactory processorManagerFactory, InboxFactory<I> inboxFactory) {
        this.clusterFactory = clusterFactory;
        this.logFactory = logFactory;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.clientSessionStoreFactory = clientSessionStoreFactory;
        this.maxClientSessions = maxClientSessions;
        this.commandExecutorFactory = commandExecutorFactory;
        this.stateMachineFactory = stateMachineFactory;
        this.electionSchedulerFactory = electionSchedulerFactory;
        this.maxBatchSize = maxBatchSize;
        this.replicationSchedulerFactory = replicationSchedulerFactory;
        this.electionTimeout = electionTimeout;
        this.snapshotterFactory = snapshotterFactory;
        this.timing = timing;
        this.processorManagerFactory = processorManagerFactory;
        this.inboxFactory = inboxFactory;
    }

    public Server<I> create(PersistentState<I> persistentState, Set<I> initialPeers) {
        return create(persistentState, initialPeers, NEVER_SNAPSHOT);
    }


    public Server<I> create(PersistentState<I> persistentState, Set<I> initialPeers, SnapshotHeuristic snapshotHeuristic) {
        Log log = logFactory.createLog(persistentState.getLogStorage());
        ClientSessionStore clientSessionStore = clientSessionStoreFactory.create(maxClientSessions);
        clientSessionStore.startListeningForClientRegistrations(log);
        StateMachine stateMachine = stateMachineFactory.createStateMachine();
        Snapshotter snapshotter = snapshotterFactory.create(log, clientSessionStore, stateMachine, persistentState, snapshotHeuristic);
        CommandExecutor commandExecutor = commandExecutorFactory.createCommandExecutor(stateMachine, clientSessionStore, snapshotter);
        commandExecutor.startListeningForCommittedCommands(log);
        ElectionScheduler electionScheduler = electionSchedulerFactory.createElectionScheduler(persistentState.getId());
        Cluster<I> cluster = clusterFactory.createCluster(persistentState.getId());
        final Configuration<I> configuration = new Configuration<>(persistentState.getId(), initialPeers, electionTimeout);
        LeadershipTransferFactory<I> leadershipTransferFactory = new LeadershipTransferFactory<>(cluster, persistentState, configuration);
        LogReplicatorFactory<I> logReplicatorFactory = new LogReplicatorFactory<>(log, persistentState, cluster, maxBatchSize);
        SnapshotReplicatorFactory<I> snapshotReplicatorFactory = new SnapshotReplicatorFactory<>(persistentState, cluster);
        ReplicationStateFactory<I> replicationStateFactory = new ReplicationStateFactory<>(log);
        SingleClientReplicatorFactory<I> singleClientReplicatorFactory = new SingleClientReplicatorFactory<>(replicationSchedulerFactory, logReplicatorFactory, snapshotReplicatorFactory, replicationStateFactory);
        log.addEntryAppendedEventHandler(configuration);
        persistentState.addSnapshotInstalledListener(configuration);
        persistentState.addSnapshotInstalledListener(log);
        persistentState.addSnapshotInstalledListener(commandExecutor);
        persistentState.addSnapshotInstalledListener(clientSessionStore);
        ReplicationManagerFactory<I> replicationManagerFactory = new ReplicationManagerFactory<>(configuration, singleClientReplicatorFactory);
        ClusterMembershipChangeManagerFactory<I> clusterMembershipChangeManagerFactory = new ClusterMembershipChangeManagerFactory<>(log,
                persistentState, configuration);
        final ProcessorManager<RaftProcessorGroup> processorManager = processorManagerFactory.create(persistentState.getId());
        final ServerStateFactory<I> idServerStateFactory = new ServerStateFactory<>(persistentState, log, cluster, pendingResponseRegistryFactory,
                clientSessionStore, commandExecutor, electionScheduler, leadershipTransferFactory, replicationManagerFactory, clusterMembershipChangeManagerFactory, timing, processorManager, configuration);
        Server<I> server = new ServerImpl<>(persistentState, idServerStateFactory, stateMachine, cluster, electionScheduler, processorManager, inboxFactory.createInbox(persistentState.getId()));
        server.initialize();
        return server;
    }
}
