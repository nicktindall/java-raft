package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.QueuedSendingStrategy;
import au.id.tindall.distalg.raft.comms.TestClusterFactory;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.SynchronousReplicationScheduler;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.serverstates.TestStateMachine;
import au.id.tindall.distalg.raft.snapshotting.Snapshotter;
import au.id.tindall.distalg.raft.state.InMemoryPersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.OK;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerInteractionTest {

    private static final byte[] COMMAND = "TheCommand".getBytes();
    private static final int MAX_CLIENT_SESSIONS = 10;
    private static final int MAX_BATCH_SIZE = 10;
    private static final int LAST_RESPONSE_RECEIVED = -1;
    private static final Set<Long> ALL_SERVER_IDS = Set.of(1L, 2L, 3L);

    private Server<Long> server1;
    private Server<Long> server2;
    private Server<Long> server3;
    private Map<Long, Server<Long>> allServers;
    private QueuedSendingStrategy queuedSendingStrategy;
    private ServerFactory<Long> serverFactory;
    @Mock
    private ElectionScheduler<Long> electionScheduler;
    @Mock
    private ElectionSchedulerFactory<Long> electionSchedulerFactory;

    @BeforeEach
    void setUp() {
        setUpFactories();
        server1 = createAndAddServer(1L);
        server2 = createAndAddServer(2L);
        server3 = createAndAddServer(3L);
    }

    private void setUpFactories() {
        allServers = new HashMap<>();
        queuedSendingStrategy = new QueuedSendingStrategy();
        when(electionSchedulerFactory.createElectionScheduler(anyLong())).thenReturn(electionScheduler);
        ClientSessionStoreFactory clientSessionStoreFactory = new ClientSessionStoreFactory();
        serverFactory = new ServerFactory<>(
                new TestClusterFactory(queuedSendingStrategy, allServers),
                new LogFactory(),
                new PendingResponseRegistryFactory(),
                clientSessionStoreFactory,
                MAX_CLIENT_SESSIONS,
                new CommandExecutorFactory(),
                TestStateMachine::new,
                electionSchedulerFactory,
                MAX_BATCH_SIZE,
                id -> new SynchronousReplicationScheduler(),
                Duration.ZERO,
                Snapshotter::new
        );
    }

    private Server<Long> createAndAddServer(long id) {
        Server<Long> server = serverFactory.create(new InMemoryPersistentState<>(id), ALL_SERVER_IDS);
        server.start();
        allServers.put(id, server);
        return server;
    }

    @Test
    void singleElectionTimeout_WillResultInUnanimousLeaderElection() {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getState()).contains(LEADER);
        assertThat(server2.getState()).contains(FOLLOWER);
        assertThat(server3.getState()).contains(FOLLOWER);
    }

    @Test
    void singleElectionTimeout_WillResultInLeaderElection_AfterSplitElection() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        server2.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getState()).contains(FOLLOWER);
        assertThat(server2.getState()).contains(LEADER);
        assertThat(server3.getState()).contains(FOLLOWER);
    }

    @Test
    void concurrentElectionTimeout_WillResultInNoLeaderElection_WhenNoQuorumIsReached() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getState()).contains(CANDIDATE);
        assertThat(server2.getState()).contains(CANDIDATE);
        assertThat(server3.getState()).contains(CANDIDATE);
    }

    @Test
    void concurrentElectionTimeout_WillElectALeader_WhenAQuorumIsReached() {
        server1.electionTimeout();
        server3.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getState()).contains(LEADER);
        assertThat(server2.getState()).contains(FOLLOWER);
        assertThat(server3.getState()).contains(FOLLOWER);
    }

    @Test
    void clientRegistrationRequest_WillReplicateClientRegistrationToAllServers() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        CompletableFuture<? extends ClientResponseMessage> handle = server1.handle(new RegisterClientRequest<>(server1.getId()));
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(handle.get()).usingRecursiveComparison().isEqualTo(new RegisterClientResponse<>(OK, 1, null));
    }

    @Test
    public void commitIndicesWillAdvanceAsLogEntriesAreDistributed() {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getLog().getCommitIndex()).isEqualTo(1);
        assertThat(server2.getLog().getCommitIndex()).isEqualTo(1);
        assertThat(server3.getLog().getCommitIndex()).isEqualTo(1);
    }

    @Test
    void clientSessionsAreCreatedAsRegistrationsAreDistributed() {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getClientSessionStore().hasSession(1)).isTrue();
        assertThat(server2.getClientSessionStore().hasSession(1)).isTrue();
        assertThat(server3.getClientSessionStore().hasSession(1)).isTrue();
    }

    @Test
    void clientRequestRequest_WillCauseStateMachinesToBeUpdated() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        queuedSendingStrategy.fullyFlush(allServers);
        CompletableFuture<? extends ClientResponseMessage> requestResponse = server1.handle(new ClientRequestRequest<>(server1.getId(), 1, 0, LAST_RESPONSE_RECEIVED, COMMAND));
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(requestResponse.get()).usingRecursiveComparison().isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.OK, new byte[]{(byte) 1}, null));
        assertThat(((TestStateMachine) server1.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server2.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server3.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
    }

    @Test
    void followers_WillReturnCorrectLeaderHintAfterElection() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        CompletableFuture<? extends ClientResponseMessage> response = server3.handle(new RegisterClientRequest<>(server3.getId()));
        assertThat(response.get()).usingRecursiveComparison().isEqualTo(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, server1.getId()));
    }

    @Test
    void duplicateCommands_WillOnlyExecuteOnce() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        queuedSendingStrategy.fullyFlush(allServers);
        CompletableFuture<? extends ClientResponseMessage> firstResponse = server1.handle(new ClientRequestRequest<>(server1.getId(), 1, 0, LAST_RESPONSE_RECEIVED, COMMAND));
        CompletableFuture<? extends ClientResponseMessage> secondResponse = server1.handle(new ClientRequestRequest<>(server1.getId(), 1, 0, LAST_RESPONSE_RECEIVED, COMMAND));
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(firstResponse.get()).usingRecursiveComparison().isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.OK, new byte[]{(byte) 1}, null));
        assertThat(secondResponse.get()).usingRecursiveComparison().isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.OK, new byte[]{(byte) 1}, null));
        assertThat(((TestStateMachine) server1.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server2.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server3.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
    }

    @Test
    void transferLeadership_WillTransferLeadershipToAnotherServer() {
        server1.electionTimeout();
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getState()).contains(LEADER);
        assertThat(server2.getState()).contains(FOLLOWER);
        assertThat(server3.getState()).contains(FOLLOWER);
        server1.transferLeadership();
        queuedSendingStrategy.fullyFlush(allServers);
        assertThat(server1.getState()).contains(FOLLOWER);
    }
}