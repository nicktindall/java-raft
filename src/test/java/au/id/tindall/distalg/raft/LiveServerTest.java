package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.comms.LiveDelayedSendingStrategy;
import au.id.tindall.distalg.raft.comms.TestClusterFactory;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounter;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounterClient;
import au.id.tindall.distalg.raft.replication.HeartbeatReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.replication.LogReplicatorFactory;
import au.id.tindall.distalg.raft.state.FileBasedPersistentState;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
    private Map<Long, PersistentState<Long>> allPersistentStates;
    private ServerFactory<Long> serverFactory;
    private LiveDelayedSendingStrategy liveDelayedSendingStrategy;
    @TempDir
    Path stateFileDirectory;

    @BeforeEach
    void setUp() {
        setUpFactories();
        createServerAndState(1L);
        createServerAndState(2L);
        createServerAndState(3L);
        startServers();
    }

    private void setUpFactories() {
        allServers = new ConcurrentHashMap<>();
        allPersistentStates = new HashMap<>();
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
                new ElectionSchedulerFactory<>(MINIMUM_ELECTION_TIMEOUT_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS)
        );
    }

    private void createServerAndState(long id) {
        PersistentState<Long> persistentState = FileBasedPersistentState.create(stateFileDirectory.resolve(String.valueOf(id)), id);
        Server<Long> server = serverFactory.create(persistentState);
        allServers.put(id, server);
        allPersistentStates.put(id, persistentState);
    }

    @AfterEach
    void tearDown() {
        liveDelayedSendingStrategy.stop();
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
        waitForAllServersToCatchUp();
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
        waitForAllServersToCatchUp();
    }

    private void waitForAllServersToCatchUp() {
        allServers.values().forEach(
                server -> await().atMost(1, MINUTES).until(() -> serverHasCaughtUp(server))
        );
    }

    private boolean serverHasCaughtUp(Server<Long> server) {
        MonotonicCounter counter = (MonotonicCounter) server.getStateMachine();
        return counter.getCounter().intValue() == COUNT_UP_TARGET;
    }

    private void killThenResurrectCurrentLeader() {
        Server<Long> currentLeader = getLeader().orElseThrow();
        Long killedServerId = currentLeader.getId();
        PersistentState<Long> currentLeaderState = allPersistentStates.get(killedServerId);
        LOGGER.warn("Killing server " + killedServerId);
        currentLeader.stop();
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);

        // Start a new node pointing to the same persistent state
        Server<Long> newCurrentLeader = serverFactory.create(currentLeaderState);
        allServers.put(killedServerId, newCurrentLeader);
        newCurrentLeader.start();
        LOGGER.warn("Server " + killedServerId + " restarted");
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
        allServers.values().forEach(Server::stop);
    }
}
