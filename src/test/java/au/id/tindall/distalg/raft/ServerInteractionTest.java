package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.OK;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import au.id.tindall.distalg.raft.client.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.comms.TestCluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServerInteractionTest {

    private Server<Long> server1;
    private Server<Long> server2;
    private Server<Long> server3;
    private TestCluster cluster;

    @BeforeEach
    void setUp() {
        PendingResponseRegistryFactory pendingResponseRegistryFactory = new PendingResponseRegistryFactory();
        LogReplicatorFactory logReplicatorFactory = new LogReplicatorFactory();
        cluster = new TestCluster();
        server1 = new Server<>(1L, new ServerStateFactory<>(1L, new Log(), cluster.forServer(1L), pendingResponseRegistryFactory, logReplicatorFactory));
        server2 = new Server<>(2L, new ServerStateFactory<>(2L, new Log(), cluster.forServer(2L), pendingResponseRegistryFactory, logReplicatorFactory));
        server3 = new Server<>(3L, new ServerStateFactory<>(3L, new Log(), cluster.forServer(3L), pendingResponseRegistryFactory, logReplicatorFactory));
        cluster.setServers(server1, server2, server3);
    }

    @Test
    void singleElectionTimeout_WillResultInUnanimousLeaderElection() {
        server1.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(LEADER);
        assertThat(server2.getState()).isEqualTo(FOLLOWER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    void singleElectionTimeout_WillResultInLeaderElection_AfterSplitElection() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        cluster.fullyFlush();
        server2.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(FOLLOWER);
        assertThat(server2.getState()).isEqualTo(LEADER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    void concurrentElectionTimeout_WillResultInNoLeaderElection_WhenNoQuorumIsReached() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(CANDIDATE);
        assertThat(server2.getState()).isEqualTo(CANDIDATE);
        assertThat(server3.getState()).isEqualTo(CANDIDATE);
    }

    @Test
    void concurrentElectionTimeout_WillElectALeader_WhenAQuorumIsReached() {
        server1.electionTimeout();
        server3.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(LEADER);
        assertThat(server2.getState()).isEqualTo(FOLLOWER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    void clientRegistrationRequest_WillReplicateClientRegistrationToAllServers() throws ExecutionException, InterruptedException {
        server1.electionTimeout();
        cluster.fullyFlush();
        CompletableFuture<? extends ClientResponseMessage> handle = server1.handle(new RegisterClientRequest<>(server1.getId()));
        cluster.fullyFlush();
        assertThat(handle.get()).isEqualToComparingFieldByFieldRecursively(new RegisterClientResponse<>(OK, 1, null));
    }
}