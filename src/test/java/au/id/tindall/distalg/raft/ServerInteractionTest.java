package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.TestClusterFactory;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestRequest;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.serverstates.TestStateMachine;
import au.id.tindall.distalg.raft.statemachine.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.OK;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static org.assertj.core.api.Assertions.assertThat;

public class ServerInteractionTest {

    private static final byte[] COMMAND = "TheCommand".getBytes();
    private static final int MAX_CLIENT_SESSIONS = 10;

    private Server<Long> server1;
    private Server<Long> server2;
    private Server<Long> server3;
    private TestClusterFactory clusterFactory;

    @BeforeEach
    void setUp() {
        PendingResponseRegistryFactory pendingResponseRegistryFactory = new PendingResponseRegistryFactory();
        LogReplicatorFactory<Long> logReplicatorFactory = new LogReplicatorFactory<>();
        LogFactory logFactory = new LogFactory();
        clusterFactory = new TestClusterFactory();
        ClientSessionStoreFactory clientSessionStoreFactory = new ClientSessionStoreFactory();
        ServerFactory<Long> serverFactory = new ServerFactory<>(clusterFactory, logFactory, pendingResponseRegistryFactory, logReplicatorFactory, clientSessionStoreFactory, MAX_CLIENT_SESSIONS,
                new CommandExecutorFactory(), TestStateMachine::new);
        server1 = serverFactory.create(1L);
        server2 = serverFactory.create(2L);
        server3 = serverFactory.create(3L);
        clusterFactory.setServers(server1, server2, server3);
    }

    @Test
    void singleElectionTimeout_WillResultInUnanimousLeaderElection() {
        server1.electionTimeout();
        clusterFactory.fullyFlush();
        assertThat(server1.getState()).isEqualTo(LEADER);
        assertThat(server2.getState()).isEqualTo(FOLLOWER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    void singleElectionTimeout_WillResultInLeaderElection_AfterSplitElection() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        clusterFactory.fullyFlush();
        server2.electionTimeout();
        clusterFactory.fullyFlush();
        assertThat(server1.getState()).isEqualTo(FOLLOWER);
        assertThat(server2.getState()).isEqualTo(LEADER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    void concurrentElectionTimeout_WillResultInNoLeaderElection_WhenNoQuorumIsReached() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        clusterFactory.fullyFlush();
        assertThat(server1.getState()).isEqualTo(CANDIDATE);
        assertThat(server2.getState()).isEqualTo(CANDIDATE);
        assertThat(server3.getState()).isEqualTo(CANDIDATE);
    }

    @Test
    void concurrentElectionTimeout_WillElectALeader_WhenAQuorumIsReached() {
        server1.electionTimeout();
        server3.electionTimeout();
        clusterFactory.fullyFlush();
        assertThat(server1.getState()).isEqualTo(LEADER);
        assertThat(server2.getState()).isEqualTo(FOLLOWER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    void clientRegistrationRequest_WillReplicateClientRegistrationToAllServers() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        clusterFactory.fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> handle = server1.handle(new RegisterClientRequest<>(server1.getId()));
        clusterFactory.fullyFlush();
        assertThat(handle.get()).isEqualToComparingFieldByFieldRecursively(new RegisterClientResponse<>(OK, 1, null));
    }

    @Test
    public void commitIndicesWillAdvanceAsLogEntriesAreDistributed() {
        server1.electionTimeout();
        clusterFactory.fullyFlush();
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        clusterFactory.fullyFlush();
        assertThat(server1.getLog().getCommitIndex()).isEqualTo(1);
        assertThat(server2.getLog().getCommitIndex()).isEqualTo(1);
        assertThat(server3.getLog().getCommitIndex()).isEqualTo(1);
    }

    @Test
    public void clientSessionsAreCreatedAsRegistrationsAreDistributed() {
        server1.electionTimeout();
        clusterFactory.fullyFlush();
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        clusterFactory.fullyFlush();
        assertThat(server1.getClientSessionStore().hasSession(1)).isTrue();
        assertThat(server2.getClientSessionStore().hasSession(1)).isTrue();
        assertThat(server3.getClientSessionStore().hasSession(1)).isTrue();
    }

    @Test
    void clientRequestRequest_WillCauseStateMachinesToBeUpdated() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        clusterFactory.fullyFlush();
        server1.handle(new RegisterClientRequest<>(server1.getId()));
        clusterFactory.fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> requestResponse = server1.handle(new ClientRequestRequest<>(server1.getId(), 1, 0, COMMAND));
        clusterFactory.fullyFlush();
        assertThat(requestResponse.get()).isEqualToComparingFieldByFieldRecursively(new ClientRequestResponse<>(ClientRequestStatus.OK, new byte[]{(byte) 1}, null));
        assertThat(((TestStateMachine) server1.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server2.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
        assertThat(((TestStateMachine) server3.getStateMachine()).getAppliedCommands()).containsExactly(COMMAND);
    }

    @Test
    void followers_WillReturnCorrectLeaderHintAfterElection() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        clusterFactory.fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> response = server3.handle(new RegisterClientRequest<>(server3.getId()));
        assertThat(response.get()).isEqualToComparingFieldByFieldRecursively(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, server1.getId()));
    }
}