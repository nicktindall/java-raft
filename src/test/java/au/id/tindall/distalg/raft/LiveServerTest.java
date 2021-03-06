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
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerRequest;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

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
    private static final Set<Long> ALL_SERVER_IDS = Set.of(1L, 2L, 3L);

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
        for (long serverId : ALL_SERVER_IDS) {
            createServerAndState(serverId, ALL_SERVER_IDS);
        }
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
                new ClientSessionStoreFactory(),
                MAX_CLIENT_SESSIONS,
                new CommandExecutorFactory(),
                MonotonicCounter::new,
                new ElectionSchedulerFactory<>(testExecutorService, MINIMUM_ELECTION_TIMEOUT_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS),
                MAX_BATCH_SIZE,
                new HeartbeatReplicationSchedulerFactory(DELAY_BETWEEN_HEARTBEATS_MILLISECONDS),
                Duration.ofMillis(MINIMUM_ELECTION_TIMEOUT_MILLISECONDS)
        );
    }

    private Server<Long> createServerAndState(long id, Set<Long> serverIds) {
        try {
            PersistentState<Long> persistentState = FileBasedPersistentState.createOrOpen(stateFileDirectory.resolve(String.valueOf(id)), id);
            Server<Long> server = serverFactory.create(persistentState, serverIds);
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

    @Test
    void willProgressWithClusterMembershipChanges() throws ExecutionException, InterruptedException {
        Future<?> counterClientThread = testExecutorService.submit(() -> {
            try {
                countUp();
            } catch (ExecutionException | InterruptedException ex) {
                fail(ex);
            }
        });

        AtomicLong newServerIdCounter = new AtomicLong(ALL_SERVER_IDS.size() + 1);
        final ScheduledFuture<?> clusterChanger = testExecutorService.scheduleAtFixedRate(addOrRemoveAServer(newServerIdCounter), 3, 3, SECONDS);
        counterClientThread.get();
        clusterChanger.cancel(false);
        try {
            clusterChanger.get();
        } catch (CancellationException ex) {
            // This is fine
        }
        waitForAllServersToCatchUp();
    }

    private Runnable addOrRemoveAServer(AtomicLong newServerIdCounter) {
        return () -> {
            if (allServers.size() <= 3) {
                addNewServer(newServerIdCounter.getAndIncrement());
            } else {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    addNewServer(newServerIdCounter.getAndIncrement());
                } else {
                    removeRandomServer();
                }
            }
        };
    }

    private void addNewServer(long newServerId) {
        try {
            Set<Long> newServersView = new HashSet<>(allServers.keySet());
            newServersView.add(newServerId);
            final Server<Long> server = createServerAndState(newServerId, newServersView);

            while (true) {
                final Optional<Server<Long>> leader = getLeader();
                if (leader.isPresent()) {
                    LOGGER.warn("Adding server {}", newServerId);
                    server.start();
                    final AddServerResponse response = (AddServerResponse) leader.get().handle(new AddServerRequest<>(newServerId)).get();
                    LOGGER.warn("Server {} added response: {}", newServerId, response.getStatus());
                    break;
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeRandomServer() {
        try {
            Server<Long> server = chooseRandomServer();
            while (true) {
                final Optional<Server<Long>> leader = getLeader();
                if (leader.isPresent()) {
                    LOGGER.warn("Removing server {}", server.getId());
                    final RemoveServerResponse response = (RemoveServerResponse) leader.get().handle(new RemoveServerRequest<>(server.getId())).get();
                    LOGGER.warn("Server {} remove response: {}", server.getId(), response.getStatus());
                    server.stop();
                    allServers.remove(server.getId());
                    break;
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Server<Long> chooseRandomServer() {
        final List<Server<Long>> servers = new ArrayList<>(allServers.values());
        while (true) {
            final Server<Long> server = servers.get(ThreadLocalRandom.current().nextInt(servers.size()));
            final Optional<ServerStateType> state = server.getState();
            if (state.isPresent() && state.get() != LEADER) {
                return server;
            }
        }
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
        Server<Long> newCurrentLeader = createServerAndState(killedServerId, ALL_SERVER_IDS);
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
