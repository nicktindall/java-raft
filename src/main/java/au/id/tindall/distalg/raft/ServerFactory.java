package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.driver.ElectionScheduler;
import au.id.tindall.distalg.raft.driver.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.driver.HeartbeatScheduler;
import au.id.tindall.distalg.raft.driver.HeartbeatSchedulerFactory;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import au.id.tindall.distalg.raft.statemachine.StateMachineFactory;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
    private final HeartbeatSchedulerFactory<ID> heartbeatSchedulerFactory;

    public ServerFactory(ClusterFactory<ID> clusterFactory, LogFactory logFactory, PendingResponseRegistryFactory pendingResponseRegistryFactory,
                         LogReplicatorFactory<ID> logReplicatorFactory, ClientSessionStoreFactory clientSessionStoreFactory, int maxClientSessions,
                         CommandExecutorFactory commandExecutorFactory, StateMachineFactory stateMachineFactory, ElectionSchedulerFactory<ID> electionSchedulerFactory, HeartbeatSchedulerFactory<ID> heartbeatSchedulerFactory) {
        this.clusterFactory = clusterFactory;
        this.logFactory = logFactory;
        this.pendingResponseRegistryFactory = pendingResponseRegistryFactory;
        this.logReplicatorFactory = logReplicatorFactory;
        this.clientSessionStoreFactory = clientSessionStoreFactory;
        this.maxClientSessions = maxClientSessions;
        this.commandExecutorFactory = commandExecutorFactory;
        this.stateMachineFactory = stateMachineFactory;
        this.electionSchedulerFactory = electionSchedulerFactory;
        this.heartbeatSchedulerFactory = heartbeatSchedulerFactory;
    }

    public Server<ID> create(ID id) {
        Log log = logFactory.createLog();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        ClientSessionStore clientSessionStore = clientSessionStoreFactory.create(maxClientSessions);
        clientSessionStore.startListeningForClientRegistrations(log);
        StateMachine stateMachine = stateMachineFactory.createStateMachine();
        CommandExecutor commandExecutor = commandExecutorFactory.createCommandExecutor(stateMachine, clientSessionStore);
        commandExecutor.startListeningForCommittedCommands(log);
        ElectionScheduler<ID> electionScheduler = electionSchedulerFactory.createElectionScheduler(scheduledExecutorService);
        HeartbeatScheduler<ID> heartbeatScheduler = heartbeatSchedulerFactory.createHeartbeatScheduler(scheduledExecutorService);
        Server<ID> server = new Server<>(id, new ServerStateFactory<>(id, log, clusterFactory.createForNode(id), pendingResponseRegistryFactory,
                logReplicatorFactory, clientSessionStore, commandExecutor, electionScheduler, heartbeatScheduler), stateMachine);
        electionScheduler.setServer(server);
        return server;
    }
}
