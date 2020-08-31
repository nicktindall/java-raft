package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.LiveDelayedSendingStrategy;
import au.id.tindall.distalg.raft.comms.TestClusterFactory;
import au.id.tindall.distalg.raft.driver.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounter;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounterClient;
import au.id.tindall.distalg.raft.replication.HeartbeatReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.replication.ReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.state.FileBasedPersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

class LiveServerTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MINIMUM_MESSAGE_DELAY = 1;
    private static final int MAXIMUM_MESSAGE_DELAY = 5;

    private static final int MAX_CLIENT_SESSIONS = 10;
    private static final int DELAY_BETWEEN_HEARTBEATS_MILLISECONDS = 200;
    private static final int MINIMUM_ELECTION_TIMEOUT_MILLISECONDS = 300;
    private static final int MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS = 500;
    private static final int COUNT_UP_TARGET = 5_000;

    private static final int MAX_BATCH_SIZE = 20;

    private Server<Long> server1;
    private Server<Long> server2;
    private Server<Long> server3;
    private TestClusterFactory clusterFactory;

    @BeforeEach
    void setUp() throws IOException {
        PendingResponseRegistryFactory pendingResponseRegistryFactory = new PendingResponseRegistryFactory();
        ReplicationSchedulerFactory replicationSchedulerFactory = new HeartbeatReplicationSchedulerFactory(DELAY_BETWEEN_HEARTBEATS_MILLISECONDS);
        LogReplicatorFactory<Long> logReplicatorFactory = new LogReplicatorFactory<>(MAX_BATCH_SIZE, replicationSchedulerFactory);
        LogFactory logFactory = new LogFactory();
        clusterFactory = new TestClusterFactory(new LiveDelayedSendingStrategy(MINIMUM_MESSAGE_DELAY, MAXIMUM_MESSAGE_DELAY));
        ClientSessionStoreFactory clientSessionStoreFactory = new ClientSessionStoreFactory();
        ServerFactory<Long> serverFactory = new ServerFactory<>(clusterFactory, logFactory, pendingResponseRegistryFactory, logReplicatorFactory, clientSessionStoreFactory, MAX_CLIENT_SESSIONS,
                new CommandExecutorFactory(), MonotonicCounter::new, new ElectionSchedulerFactory<>(MINIMUM_ELECTION_TIMEOUT_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS));
        Path tempDir = Files.createTempDirectory("LiveServerTest");
        server1 = serverFactory.create(FileBasedPersistentState.create(tempDir.resolve("one"), 1L));
        server2 = serverFactory.create(FileBasedPersistentState.create(tempDir.resolve("two"), 2L));
        server3 = serverFactory.create(FileBasedPersistentState.create(tempDir.resolve("three"), 3L));
        clusterFactory.setServers(server1, server2, server3);
        startServers();
    }

    @AfterEach
    void tearDown() {
        clusterFactory.logStats();
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

    @Test
    void willProgressWithNoFailures() throws ExecutionException, InterruptedException {
        countUp();
        await().atMost(1, MINUTES).until(() -> serverHasCaughtUp(server1));
        await().atMost(1, MINUTES).until(() -> serverHasCaughtUp(server2));
        await().atMost(1, MINUTES).until(() -> serverHasCaughtUp(server3));
    }

    @Test
    void willProgressWithFailures() throws InterruptedException, ExecutionException {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);
        Future<?> counterClientThread = executorService.submit(() -> {
            try {
                countUp();
            } catch (ExecutionException | InterruptedException ex) {
                fail(ex);
            }
        });
        ScheduledFuture<?> periodicLeaderKiller = executorService.scheduleAtFixedRate(this::killThenResurrectCurrentLeader, 5, 5, SECONDS);

        counterClientThread.get();
        periodicLeaderKiller.cancel(false);
        await().atMost(1, MINUTES).until(() -> serverHasCaughtUp(server1));
        await().atMost(1, MINUTES).until(() -> serverHasCaughtUp(server2));
        await().atMost(1, MINUTES).until(() -> serverHasCaughtUp(server3));
    }

    private boolean serverHasCaughtUp(Server<Long> server) {
        MonotonicCounter counter = (MonotonicCounter) server.getStateMachine();
        return counter.getCounter().intValue() == COUNT_UP_TARGET;
    }

    private void killThenResurrectCurrentLeader() {
        Server<Long> currentLeader = getLeader().get();
        LOGGER.warn("Killing server " + currentLeader.getId());
        currentLeader.stop();
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);
        LOGGER.warn("Server " + currentLeader.getId() + " restarted");
        currentLeader.start();
    }

    private void countUp() throws ExecutionException, InterruptedException {
        MonotonicCounterClient counterClient = new MonotonicCounterClient(List.of(server1, server2, server3));
        counterClient.register();
        for (int i = 0; i < COUNT_UP_TARGET; i++) {
            counterClient.increment();
        }
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
