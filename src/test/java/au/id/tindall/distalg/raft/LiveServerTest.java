package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.LiveDelayedSendingStrategy;
import au.id.tindall.distalg.raft.comms.TestClusterFactory;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounter;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounterClient;
import au.id.tindall.distalg.raft.replication.HeartbeatReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.state.FileBasedPersistentState;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import au.id.tindall.distalg.raft.threading.NamedThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
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

    private TestClusterFactory clusterFactory;
    private Map<Long, Server<Long>> allServers;
    private ServerFactory<Long> serverFactory;
    private LiveDelayedSendingStrategy liveDelayedSendingStrategy;
    private ScheduledExecutorService testExecutorService;
    @TempDir
    Path stateFileDirectory;

    @BeforeEach
    void setUp() {
        testExecutorService = Executors.newScheduledThreadPool(10, new NamedThreadFactory("test-threads"));
        setUpFactories();
        createServerAndState(1L);
        createServerAndState(2L);
        createServerAndState(3L);
        startServers();
    }

    private void setUpFactories() {
        allServers = new ConcurrentHashMap<>();
        liveDelayedSendingStrategy = new LiveDelayedSendingStrategy(MINIMUM_MESSAGE_DELAY, MAXIMUM_MESSAGE_DELAY);
        clusterFactory = new TestClusterFactory(liveDelayedSendingStrategy, allServers);
        serverFactory = new ServerFactory<>(
                clusterFactory,
                new LogFactory(),
                new PendingResponseRegistryFactory(),
                new LogReplicatorFactory<>(MAX_BATCH_SIZE, new HeartbeatReplicationSchedulerFactory(DELAY_BETWEEN_HEARTBEATS_MILLISECONDS)),
                new ClientSessionStoreFactory(),
                MAX_CLIENT_SESSIONS,
                new CommandExecutorFactory(),
                MonotonicCounter::new,
                new ElectionSchedulerFactory<>(testExecutorService, MINIMUM_ELECTION_TIMEOUT_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS)
        );
    }

    private Server<Long> createServerAndState(long id) {
        try {
            PersistentState<Long> persistentState = FileBasedPersistentState.createOrOpen(stateFileDirectory.resolve(String.valueOf(id)), id);
            Server<Long> server = serverFactory.create(persistentState);
            allServers.put(id, server);
            return server;
        } catch (IOException e) {
            throw new RuntimeException("Error creating persistent state");
        }
    }

    @AfterEach
    void tearDown() {
        stopServers();
        liveDelayedSendingStrategy.stop();
        clusterFactory.logStats();
        testExecutorService.shutdown();
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
        waitForAllServersToCatchUp();
    }

    @Test
    void willProgressWithFailures() throws InterruptedException, ExecutionException {
        Future<?> counterClientThread = testExecutorService.submit(() -> {
            try {
                countUp();
            } catch (ExecutionException | InterruptedException ex) {
                fail(ex);
            }
        });
        ScheduledFuture<?> periodicLeaderKiller = testExecutorService.scheduleAtFixedRate(this::killThenResurrectCurrentLeader, 5, 5, SECONDS);

        counterClientThread.get();
        periodicLeaderKiller.cancel(false);
        try {
            periodicLeaderKiller.get();
        } catch (CancellationException ex) {
            // This is fine
        }
        waitForAllServersToCatchUp();
    }

    @Test
    void willProgressWithLeadershipTransfers() throws ExecutionException, InterruptedException {
        Future<?> counterClientThread = testExecutorService.submit(() -> {
            try {
                countUp();
            } catch (ExecutionException | InterruptedException ex) {
                fail(ex);
            }
        });
        ScheduledFuture<?> periodicTransferTrigger = testExecutorService.scheduleAtFixedRate(this::triggerLeadershipTransfer, 5, 5, SECONDS);

        counterClientThread.get();
        periodicTransferTrigger.cancel(false);
        try {
            periodicTransferTrigger.get();
        } catch (CancellationException ex) {
            // This is fine
        }
        waitForAllServersToCatchUp();
    }

    private void waitForAllServersToCatchUp() {
        await().atMost(1, MINUTES).until(
                () -> allServers.values().stream().map(this::serverHasCaughtUp).reduce(true, (a, b) -> a && b)
        );
    }

    private boolean serverHasCaughtUp(Server<Long> server) {
        MonotonicCounter counter = (MonotonicCounter) server.getStateMachine();
        return counter.getCounter().intValue() == COUNT_UP_TARGET;
    }

    private void killThenResurrectCurrentLeader() {
        Server<Long> currentLeader = getLeader().orElseThrow();
        Long killedServerId = currentLeader.getId();
        LOGGER.warn("Killing server " + killedServerId);
        currentLeader.stop();
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);

        // Start a new node pointing to the same persistent state files
        Server<Long> newCurrentLeader = createServerAndState(killedServerId);
        allServers.put(killedServerId, newCurrentLeader);
        newCurrentLeader.start();
        LOGGER.warn("Server " + killedServerId + " restarted");
    }

    private void triggerLeadershipTransfer() {
        Server<Long> currentLeader = getLeader().orElseThrow();
        long currentLeaderId = currentLeader.getId();
        LOGGER.warn("Telling server {} to transfer leadership", currentLeaderId);
        currentLeader.transferLeadership();
        await().atMost(10, SECONDS).until(() -> this.serverIsNoLongerLeader(currentLeaderId));
    }

    private boolean serverIsNoLongerLeader(long serverId) {
        return getLeader()
                .filter(server -> server.getId() != serverId)
                .isPresent();
    }

    private void countUp() throws ExecutionException, InterruptedException {
        MonotonicCounterClient counterClient = new MonotonicCounterClient(allServers);
        counterClient.register();
        for (int i = 0; i < COUNT_UP_TARGET; i++) {
            counterClient.increment();
        }
    }

    private boolean aLeaderIsElected() {
        return getLeader().isPresent();
    }

    private Optional<Server<Long>> getLeader() {
        return allServers.values().stream()
                .filter(server -> server.getState().isPresent() && server.getState().get() == LEADER)
                .findAny();
    }

    private void startServers() {
        allServers.values().forEach(Server::start);
    }

    private void stopServers() {
        allServers.values().forEach(server -> {
            try {
                server.stop();
            } catch (NotRunningException e) {
                // This is fine, leader killer might have already done the job for us
            }
        });
    }
}
