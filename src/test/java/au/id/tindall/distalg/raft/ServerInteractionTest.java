package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.ServerState.CANDIDATE;
import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;
import static au.id.tindall.distalg.raft.ServerState.LEADER;
import static org.assertj.core.api.Assertions.assertThat;

import au.id.tindall.distalg.raft.comms.TestCluster;
import org.junit.Before;
import org.junit.Test;

public class ServerInteractionTest {

    private Server<Long> server1;
    private Server<Long> server2;
    private Server<Long> server3;
    private TestCluster cluster;

    @Before
    public void setUp() {
        cluster = new TestCluster();
        server1 = new Server<>(1L, cluster);
        server2 = new Server<>(2L, cluster);
        server3 = new Server<>(3L, cluster);
        cluster.setServers(server1, server2, server3);
    }

    @Test
    public void singleElectionTimeoutResultsInUnanimousLeaderElection() {
        server1.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(LEADER);
        assertThat(server2.getState()).isEqualTo(FOLLOWER);
        assertThat(server3.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    public void concurrentElectionTimeoutResultsInNoLeaderElection() {
        server1.electionTimeout();
        server2.electionTimeout();
        server3.electionTimeout();
        cluster.fullyFlush();
        assertThat(server1.getState()).isEqualTo(CANDIDATE);
        assertThat(server2.getState()).isEqualTo(CANDIDATE);
        assertThat(server3.getState()).isEqualTo(CANDIDATE);
    }
}