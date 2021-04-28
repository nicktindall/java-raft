package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.serverstates.leadershiptransfer.LeadershipTransferFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import au.id.tindall.distalg.raft.statemachine.StateMachineFactory;

import java.io.Serializable;

public class ServerFactory<ID extends Serializable> {

    private final ClusterFactory<ID> clusterFactory;
    private final LogFactory logFactory;
    private final PendingResponseRegistryFactory pendingResponseRegistryFactory;
    private final LogReplicatorFactory<ID> logReplicatorFactory;
    private final ClientSessionStoreFactory clientSessionStoreFactory;
    private final int maxClientSessions;
    private final CommandExecutorFactory commandExecutorFactory;
    private final StateMachineFactory stateMachineFactory;
    private final ElectionSchedulerFactory<ID> electionSchedulerFactory;

    public ServerFactory(ClusterFactory<ID> clusterFactory, LogFactory logFactory, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                         LogReplicatorFactory<ID> logReplicatorFactory, ClientSessionStoreFactory clientSessionStoreFactory, int maxClientSessions,
                         CommandExecutorFactory commandExecutorFactory, StateMachineFactory stateMachineFactory, ElectionSchedulerFactory<ID> electionSchedulerFactory) {
        this.clusterFactory = clusterFactory;
        this.logFactory = logFactory;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.clientSessionStoreFactory = clientSessionStoreFactory;
        this.maxClientSessions = maxClientSessions;
        this.commandExecutorFactory = commandExecutorFactory;
        this.stateMachineFactory = stateMachineFactory;
        this.electionSchedulerFactory = electionSchedulerFactory;
    }

    public Server<ID> create(PersistentState<ID> persistentState) {
        Log log = logFactory.createLog(persistentState.getLogStorage());
        ClientSessionStore clientSessionStore = clientSessionStoreFactory.create(maxClientSessions);
        clientSessionStore.startListeningForClientRegistrations(log);
        StateMachine stateMachine = stateMachineFactory.createStateMachine();
        CommandExecutor commandExecutor = commandExecutorFactory.createCommandExecutor(stateMachine, clientSessionStore);
        commandExecutor.startListeningForCommittedCommands(log);
        ElectionScheduler<ID> electionScheduler = electionSchedulerFactory.createElectionScheduler();
        Cluster<ID> cluster = clusterFactory.createForNode(persistentState.getId());
        LeadershipTransferFactory<ID> leadershipTransferFactory = new LeadershipTransferFactory<>(cluster, persistentState);
        Server<ID> server = new Server<>(persistentState, new ServerStateFactory<>(persistentState, log, cluster, pendingResponseRegistryFactory,
                logReplicatorFactory, clientSessionStore, commandExecutor, electionScheduler, leadershipTransferFactory), stateMachine);
        electionScheduler.setServer(server);
        return server;
    }
}
