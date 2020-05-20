package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.LiveDelayedSendingStrategy;
import au.id.tindall.distalg.raft.comms.TestClusterFactory;
import au.id.tindall.distalg.raft.driver.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.driver.HeartbeatSchedulerFactory;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.serverstates.TestStateMachine;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class LiveServerTest {

    private static final int MINIMUM_MESSAGE_DELAY = 5;
    private static final int MAXIMUM_MESSAGE_DELAY = 20;

    private static final int MAX_CLIENT_SESSIONS = 10;
    private static final int DELAY_BETWEEN_HEARTBEATS_MILLISECONDS = 200;
    private static final int MINIMUM_ELECTION_TIMEOUT_MILLISECONDS = 300;
    private static final int MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS = 500;

    private Server<Long> server1;
    private Server<Long> server2;
    private Server<Long> server3;
    private TestClusterFactory clusterFactory;

    @BeforeEach
    public void setUp() {
        PendingResponseRegistryFactory pendingResponseRegistryFactory = new PendingResponseRegistryFactory();
        LogReplicatorFactory<Long> logReplicatorFactory = new LogReplicatorFactory<>();
        LogFactory logFactory = new LogFactory();
        clusterFactory = new TestClusterFactory(new LiveDelayedSendingStrategy(MINIMUM_MESSAGE_DELAY, MAXIMUM_MESSAGE_DELAY));
        ClientSessionStoreFactory clientSessionStoreFactory = new ClientSessionStoreFactory();
        ServerFactory<Long> serverFactory = new ServerFactory<>(clusterFactory, logFactory, pendingResponseRegistryFactory, logReplicatorFactory, clientSessionStoreFactory, MAX_CLIENT_SESSIONS,
                new CommandExecutorFactory(), TestStateMachine::new, new ElectionSchedulerFactory<>(MINIMUM_ELECTION_TIMEOUT_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS), new HeartbeatSchedulerFactory<>(DELAY_BETWEEN_HEARTBEATS_MILLISECONDS));
        server1 = serverFactory.create(1L);
        server2 = serverFactory.create(2L);
        server3 = serverFactory.create(3L);
        clusterFactory.setServers(server1, server2, server3);
        startServers();
    }

    @AfterEach
    void tearDown() {
        stopServers();
    }

    @Test
    void willElectALeader() {
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);
    }

    @Test
    void willElectANewLeader_WhenTheExistingLeaderFails() {
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);
        Server<Long> oldLeader = getLeader().get();
        oldLeader.stop();
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);
        oldLeader.start();
    }

    private boolean aLeaderIsElected() {
        return getLeader().isPresent();
    }

    private Optional<Server<Long>> getLeader() {
        return List.of(server1, server2, server3).stream()
                .filter(server -> server.getState().isPresent() && server.getState().get() == LEADER)
                .findAny();
    }

    private void startServers() {
        server1.start();
        server2.start();
        server3.start();
    }

    private void stopServers() {
        server1.stop();
        server2.stop();
        server3.stop();
    }
}
