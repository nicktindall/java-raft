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
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.replication.ReplicationManagerFactory;
import au.id.tindall.distalg.raft.replication.ReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.replication.SingleClientReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.clustermembership.ClusterMembershipChangeManagerFactory;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import au.id.tindall.distalg.raft.statemachine.StateMachineFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.Set;

public class ServerFactory<ID extends Serializable> {

    private final ClusterFactory<ID> clusterFactory;
    private final LogFactory logFactory;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final ClientSessionStoreFactory clientSessionStoreFactory;
    private final int maxClientSessions;
    private final CommandExecutorFactory commandExecutorFactory;
    private final StateMachineFactory stateMachineFactory;
    private final ElectionSchedulerFactory<ID> electionSchedulerFactory;
    private final int maxBatchSize;
    private final ReplicationSchedulerFactory replicationSchedulerFactory;
    private final Duration electionTimeout;

    public ServerFactory(ClusterFactory<ID> clusterFactory, LogFactory logFactory, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                         ClientSessionStoreFactory clientSessionStoreFactory, int maxClientSessions,
                         CommandExecutorFactory commandExecutorFactory, StateMachineFactory stateMachineFactory, ElectionSchedulerFactory<ID> electionSchedulerFactory,
                         int maxBatchSize, ReplicationSchedulerFactory replicationSchedulerFactory, Duration electionTimeout) {
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
    }

    public Server<ID> create(PersistentState<ID> persistentState, Set<ID> initialPeers) {
        Log log = logFactory.createLog(persistentState.getLogStorage());
        ClientSessionStore clientSessionStore = clientSessionStoreFactory.create(maxClientSessions);
        clientSessionStore.startListeningForClientRegistrations(log);
        StateMachine stateMachine = stateMachineFactory.createStateMachine();
        CommandExecutor commandExecutor = commandExecutorFactory.createCommandExecutor(stateMachine, clientSessionStore);
        commandExecutor.startListeningForCommittedCommands(log);
        ElectionScheduler<ID> electionScheduler = electionSchedulerFactory.createElectionScheduler();
        Cluster<ID> cluster = clusterFactory.createForNode(persistentState.getId());
        LeadershipTransferFactory<ID> leadershipTransferFactory = new LeadershipTransferFactory<>(cluster, persistentState);
        LogReplicatorFactory<ID> logReplicatorFactory = new LogReplicatorFactory<>(log, persistentState, cluster, maxBatchSize, replicationSchedulerFactory);
        SingleClientReplicatorFactory<ID> singleClientReplicatorFactory = new SingleClientReplicatorFactory<>(replicationSchedulerFactory, logReplicatorFactory);
        final Configuration<ID> configuration = new Configuration<>(persistentState.getId(), initialPeers, electionTimeout);
        log.addEntryAppendedEventHandler(configuration);
        ReplicationManagerFactory<ID> replicationManagerFactory = new ReplicationManagerFactory<>(configuration, singleClientReplicatorFactory);
        ClusterMembershipChangeManagerFactory<ID> clusterMembershipChangeManagerFactory = new ClusterMembershipChangeManagerFactory<>(log,
                persistentState, configuration);
        final ServerStateFactory<ID> idServerStateFactory = new ServerStateFactory<>(persistentState, log, cluster, configuration, pendingResponseRegistryFactory,
                clientSessionStore, commandExecutor, electionScheduler, leadershipTransferFactory, replicationManagerFactory, clusterMembershipChangeManagerFactory);
        Server<ID> server = new Server<>(persistentState, idServerStateFactory, stateMachine);
        electionScheduler.setServer(server);
        return server;
    }
}
