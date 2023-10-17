package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.QueuedSendingStrategy;
import au.id.tindall.distalg.raft.comms.TestClusterFactory;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.processors.ManualProcessorDriver;
import au.id.tindall.distalg.raft.processors.ProcessorManagerImpl;
import au.id.tindall.distalg.raft.processors.RaftProcessorGroup;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import static org.mockito.Mockito.mock;
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
    private ServerFactory<Long> serverFactory;
    @Mock
    private ElectionSchedulerFactory<Long> electionSchedulerFactory;
    private List<ManualProcessorDriver<RaftProcessorGroup>> pms;
    private Map<Long, ElectionScheduler> electionSchedulers;

    @BeforeEach
    void setUp() {
        electionSchedulers = new HashMap<>();
        pms = new ArrayList<>();
        setUpFactories();
        server1 = createAndAddServer(1L);
        server2 = createAndAddServer(2L);
        server3 = createAndAddServer(3L);
    }

    private void timeoutServer(long serverId) {
        when(electionSchedulers.get(serverId).shouldTimeout()).thenReturn(true, false);
        allServers.get(serverId).timeoutNowIfDue();
    }

    private void setUpFactories() {
        allServers = new HashMap<>();
        when(electionSchedulerFactory.createElectionScheduler(anyLong())).thenAnswer(iom -> {
            long serverId = iom.getArgument(0);
            final ElectionScheduler es = mock(ElectionScheduler.class);
            electionSchedulers.put(serverId, es);
            return es;
        });
        ClientSessionStoreFactory clientSessionStoreFactory = new ClientSessionStoreFactory();
        final TestClusterFactory testClusterFactory = new TestClusterFactory(new QueuedSendingStrategy());
        serverFactory = new ServerFactory<>(
                testClusterFactory,
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
                Snapshotter::new,
                false,
                serverId -> {
                    ManualProcessorDriver<RaftProcessorGroup> mpe = new ManualProcessorDriver<>();
                    pms.add(mpe);
                    return new ProcessorManagerImpl<>(serverId, mpe);
                },
                testClusterFactory
        );
    }

    private Server<Long> createAndAddServer(long id) {
        Server<Long> server = serverFactory.create(new InMemoryPersistentState<>(id), ALL_SERVER_IDS);
        server.start();
        allServers.put(id, server);
        fullyFlush();
        return server;
    }

    @Test
    void singleElectionTimeout_WillResultInUnanimousLeaderElection() {
        timeoutServer(1);
        fullyFlush();
        assertThat(server1.getState()).contains(LEADER);
        assertThat(server2.getState()).contains(FOLLOWER);
        assertThat(server3.getState()).contains(FOLLOWER);
    }

    @Test
    void singleElectionTimeout_WillResultInLeaderElection_AfterSplitElection() {
        timeoutServer(1);
        timeoutServer(2);
        timeoutServer(3);
        fullyFlush();
        assertThat(server1.getState()).contains(CANDIDATE);
        assertThat(server2.getState()).contains(CANDIDATE);
        assertThat(server3.getState()).contains(CANDIDATE);
        timeoutServer(2);
        fullyFlush();
        assertThat(server1.getState()).contains(FOLLOWER);
        assertThat(server2.getState()).contains(LEADER);
        assertThat(server3.getState()).contains(FOLLOWER);
    }

    @Test
    void concurrentElectionTimeout_WillResultInNoLeaderElection_WhenNoQuorumIsReached() {
        timeoutServer(1);
        timeoutServer(2);
        timeoutServer(3);
        fullyFlush();
        assertThat(server1.getState()).contains(CANDIDATE);
        assertThat(server2.getState()).contains(CANDIDATE);
        assertThat(server3.getState()).contains(CANDIDATE);
    }

    @Test
    void concurrentElectionTimeout_WillElectALeader_WhenAQuorumIsReached() {
        timeoutServer(1);
        timeoutServer(3);
        fullyFlush();
        assertThat(server1.getState()).contains(LEADER);
        assertThat(server2.getState()).contains(FOLLOWER);
        assertThat(server3.getState()).contains(FOLLOWER);
    }

    @Test
    void clientRegistrationRequest_WillReplicateClientRegistrationToAllServers() throws ExecutionException, InterruptedException {
        timeoutServer(1);
        fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> handle = server1.handle(new RegisterClientRequest<>(server1.getId()));
        fullyFlush();
        assertThat(handle.get()).usingRecursiveComparison().isEqualTo(new RegisterClientResponse<>(OK, 1, null));
    }

    @Test
    void commitIndicesWillAdvanceAsLogEntriesAreDistributed() {
        timeoutServer(1);
        fullyFlush();
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        fullyFlush();
        assertThat(server1.getLog().getCommitIndex()).isEqualTo(1);
        assertThat(server2.getLog().getCommitIndex()).isEqualTo(1);
        assertThat(server3.getLog().getCommitIndex()).isEqualTo(1);
    }

    @Test
    void clientSessionsAreCreatedAsRegistrationsAreDistributed() {
        timeoutServer(1);
        fullyFlush();
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        fullyFlush();
        assertThat(server1.getClientSessionStore().hasSession(1)).isTrue();
        assertThat(server2.getClientSessionStore().hasSession(1)).isTrue();
        assertThat(server3.getClientSessionStore().hasSession(1)).isTrue();
    }

    @Test
    void clientRequestRequest_WillCauseStateMachinesToBeUpdated() throws ExecutionException, InterruptedException {
        timeoutServer(1);
        fullyFlush();
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> requestResponse = server1.handle(new ClientRequestRequest<>(server1.getId(), 1, 0, LAST_RESPONSE_RECEIVED, COMMAND));
        fullyFlush();
        assertThat(requestResponse.get()).usingRecursiveComparison().isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.OK, new byte[]{(byte) 1}, null));
        assertThat(((TestStateMachine) server1.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server2.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server3.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
    }

    @Test
    void followers_WillReturnCorrectLeaderHintAfterElection() throws ExecutionException, InterruptedException {
        timeoutServer(1);
        fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> response = server3.handle(new RegisterClientRequest<>(server3.getId()));
        assertThat(response.get()).usingRecursiveComparison().isEqualTo(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, server1.getId()));
    }

    @Test
    void duplicateCommands_WillOnlyExecuteOnce() throws ExecutionException, InterruptedException {
        timeoutServer(1);
        fullyFlush();
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> firstResponse = server1.handle(new ClientRequestRequest<>(server1.getId(), 1, 0, LAST_RESPONSE_RECEIVED, COMMAND));
        CompletableFuture<? extends ClientResponseMessage> secondResponse = server1.handle(new ClientRequestRequest<>(server1.getId(), 1, 0, LAST_RESPONSE_RECEIVED, COMMAND));
        fullyFlush();
        assertThat(firstResponse.get()).usingRecursiveComparison().isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.OK, new byte[]{(byte) 1}, null));
        assertThat(secondResponse.get()).usingRecursiveComparison().isEqualTo(new ClientRequestResponse<>(ClientRequestStatus.OK, new byte[]{(byte) 1}, null));
        assertThat(((TestStateMachine) server1.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server2.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server3.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
    }

    @Test
    void transferLeadership_WillTransferLeadershipToAnotherServer() {
        timeoutServer(1);
        fullyFlush();
        assertThat(server1.getState()).contains(LEADER);
        assertThat(server2.getState()).contains(FOLLOWER);
        assertThat(server3.getState()).contains(FOLLOWER);
        server1.transferLeadership();
        fullyFlush();
        assertThat(server1.getState()).contains(FOLLOWER);
    }

    private void fullyFlush() {
        boolean somethingHappened = true;
        while (somethingHappened) {
            somethingHappened = false;
            for (ManualProcessorDriver<?> pm : pms) {
                final boolean flushActive = pm.flush();
                somethingHappened = somethingHappened || flushActive;
            }
        }
    }
}