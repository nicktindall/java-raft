package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.ClusterFactory;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import au.id.tindall.distalg.raft.statemachine.StateMachineFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerFactoryTest {

    private static final Long SERVER_ID = 12345L;
    public static final int MAX_CLIENT_SESSIONS = 999;

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
    private LogReplicatorFactory<Long> logReplicatorFactory;
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
    private ServerFactory<Long> serverFactory;

    @BeforeEach
    void setUp() {
        when(clientSessionStoreFactory.create(MAX_CLIENT_SESSIONS)).thenReturn(clientSessionStore);
        when(clusterFactory.createForNode(eq(SERVER_ID))).thenReturn(cluster);
        when(logFactory.createLog()).thenReturn(log);
        when(stateMachineFactory.createStateMachine()).thenReturn(stateMachine);
        when(commandExecutorFactory.createCommandExecutor(stateMachine, clientSessionStore)).thenReturn(commandExecutor);
        serverFactory = new ServerFactory<>(clusterFactory, logFactory, pendingResponseRegistryFactory, logReplicatorFactory, clientSessionStoreFactory, MAX_CLIENT_SESSIONS,
                commandExecutorFactory, stateMachineFactory);
    }

    @Test
    void createsServersAndTheirDependencies() {
        assertThat(serverFactory.create(SERVER_ID)).isEqualToComparingFieldByFieldRecursively(new Server<>(SERVER_ID, new ServerStateFactory<>(SERVER_ID,
                log, cluster, pendingResponseRegistryFactory, logReplicatorFactory, clientSessionStore, commandExecutor), stateMachine));
    }

    @Test
    void startsListeningForClientRegistrations() {
        serverFactory.create(SERVER_ID);
        verify(clientSessionStore).startListeningForClientRegistrations(log);
    }

    @Test
    void startsListeningForCommittedStateMachineCommands() {
        serverFactory.create(SERVER_ID);
        verify(commandExecutor).startListeningForCommittedCommands(log);
    }
}