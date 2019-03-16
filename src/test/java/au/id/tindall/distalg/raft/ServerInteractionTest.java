package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static org.assertj.core.api.Assertions.assertThat;

import au.id.tindall.distalg.raft.comms.TestCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServerInteractionTest {

    private Server<Long> server1;
    private Server<Long> server2;
    private Server<Long> server3;
    private TestCluster cluster;

    @BeforeEach
    public void setUp() {
        cluster = new TestCluster();
        server1 = new Server<>(1L, cluster);
        server2 = new Server<>(2L, cluster);
        server3 = new Server<>(3L, cluster);
        cluster.setServers(server1, server2, server3);
    }

    @Test
    public void singleElectionTimeout_WillResultInUnanimousLeaderElection() {
        server1.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(LEADER);
        assertThat(server2.getState()).isEqualTo(FOLLOWER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    public void singleElectionTimeout_WillResultInLeaderElection_AfterSplitElection() {
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
    public void concurrentElectionTimeout_WillResultInNoLeaderElection_WhenNoQuorumIsReached() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(CANDIDATE);
        assertThat(server2.getState()).isEqualTo(CANDIDATE);
        assertThat(server3.getState()).isEqualTo(CANDIDATE);
    }

    @Test
    public void concurrentElectionTimeout_WillElectALeader_WhenAQuorumIsReached() {
        server1.electionTimeout();
        server3.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(LEADER);
        assertThat(server2.getState()).isEqualTo(FOLLOWER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }
}