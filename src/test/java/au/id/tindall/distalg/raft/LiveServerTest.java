package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.client.responses.PendingResponseRegistryFactory;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStoreFactory;
import au.id.tindall.distalg.raft.clusterclient.ClusterAdminClient;
import au.id.tindall.distalg.raft.clusterclient.ClusterClient;
import au.id.tindall.distalg.raft.comms.TestInfrastructureFactory;
import au.id.tindall.distalg.raft.comms.simulated.NetworkSimulation;
import au.id.tindall.distalg.raft.elections.ElectionSchedulerFactory;
import au.id.tindall.distalg.raft.exceptions.NotRunningException;
import au.id.tindall.distalg.raft.log.LogFactory;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounter;
import au.id.tindall.distalg.raft.monotoniccounter.MonotonicCounterClient;
import au.id.tindall.distalg.raft.processors.ProcessorManagerFactoryImpl;
import au.id.tindall.distalg.raft.processors.SleepStrategies;
import au.id.tindall.distalg.raft.replication.HeartbeatReplicationSchedulerFactory;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import au.id.tindall.distalg.raft.serialisation.LongIDSerializer;
import au.id.tindall.distalg.raft.serverstates.ServerStateType;
import au.id.tindall.distalg.raft.snapshotting.DumbRegularIntervalSnapshotHeuristic;
import au.id.tindall.distalg.raft.snapshotting.Snapshotter;
import au.id.tindall.distalg.raft.state.FileBasedPersistentState;
import au.id.tindall.distalg.raft.state.PersistentState;
import au.id.tindall.distalg.raft.statemachine.CommandExecutorFactory;
import au.id.tindall.distalg.raft.timing.TimingWrappers;
import au.id.tindall.distalg.raft.util.Closeables;
import au.id.tindall.distalg.raft.util.ExecutorUtil;
import au.id.tindall.distalg.raft.util.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.LoggingListener;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse.Status.OK;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static au.id.tindall.distalg.raft.threading.NamedThreadFactory.forThreadGroup;
import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMillis;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

class LiveServerTest {
    private static final boolean LONG_RUN_TEST = Boolean.getBoolean("LiveServerTest.longRun");
    private static final Duration LONG_RUN_TEST_LENGTH = Duration.ofDays(1);

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MINIMUM_MESSAGE_DELAY_MICROS = 350;
    private static final int MAXIMUM_MESSAGE_DELAY_MICROS = 1500;

    private static final int MAX_CLIENT_SESSIONS = 10;
    private static final int DELAY_BETWEEN_HEARTBEATS_MILLISECONDS = 200;
    private static final int MINIMUM_ELECTION_TIMEOUT_MILLISECONDS = 300;
    private static final int MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS = 500;
    private static final int COUNT_UP_TARGET = LONG_RUN_TEST ? 1_000_000 : 5_000;
    private static final int TIMEOUT_MINUTES = LONG_RUN_TEST ? 200 : 1;

    private static final int MAX_BATCH_SIZE = 20;
    private static final Set<Long> ALL_SERVER_IDS = Set.of(1L, 2L, 3L);
    private static final float PACKET_DROP_PROBABILITY = 0.001f;    // 0.1% which is quite high
    private static final int WARNING_THRESHOLD_MILLIS = 25;

    private TestInfrastructureFactory<Long> clusterFactory;
    private Map<Long, Server<Long>> allServers;
    private ClusterClient<Long> testClusterClient;
    private ServerFactory<Long> serverFactory;
    private ScheduledExecutorService testExecutorService;
    private AtomicReference<RuntimeException> testFailure;
    private ClusterAdminClient<Long> clusterAdminClient;
    @TempDir
    Path stateFileDirectory;

    public static void main(String[] args) {
        Instant startTime = Instant.now();
        try (final PrintWriter writer = new PrintWriter(System.out)) {
            while (Instant.now().isBefore(startTime.plus(LONG_RUN_TEST_LENGTH))) {
                LauncherDiscoveryRequest ldr = LauncherDiscoveryRequestBuilder.request()
                        .selectors(selectClass(LiveServerTest.class))
                        .build();
                final Launcher launcher = LauncherFactory.create();
                launcher.discover(ldr);
                final SummaryGeneratingListener summaryGeneratingListener = new SummaryGeneratingListener();
                launcher.registerTestExecutionListeners(LoggingListener.forJavaUtilLogging(), summaryGeneratingListener);
                launcher.execute(ldr);
                final TestExecutionSummary summary = summaryGeneratingListener.getSummary();
                summary.printTo(writer);
                if (summary.getFailures().size() > 0) {
                    summary.printFailuresTo(writer);
                    break;
                }
            }
        }
    }

    @BeforeEach
    void setUp() {
        testFailure = new AtomicReference<>();
        testExecutorService = newScheduledThreadPool(10, forThreadGroup("test-threads"));
        setUpFactories();
        for (long serverId : ALL_SERVER_IDS) {
            createServerAndState(serverId, ALL_SERVER_IDS);
        }
        startServers();
        clusterAdminClient = new ClusterAdminClient<>(testClusterClient, 10_000);
    }

    private void setUpFactories() {
        allServers = new ConcurrentHashMap<>();
        clusterFactory = getInfrastructureFactory();
        testClusterClient = clusterFactory.createClusterClient();
        serverFactory = new ServerFactory<>(
                clusterFactory,
                new LogFactory(),
                new PendingResponseRegistryFactory(),
                new ClientSessionStoreFactory(),
                MAX_CLIENT_SESSIONS,
                new CommandExecutorFactory(),
                MonotonicCounter::new,
                new ElectionSchedulerFactory<>(MINIMUM_ELECTION_TIMEOUT_MILLISECONDS, MAXIMUM_ELECTION_TIMEOUT_MILLISECONDS),
                MAX_BATCH_SIZE,
                new HeartbeatReplicationSchedulerFactory<>(DELAY_BETWEEN_HEARTBEATS_MILLISECONDS),
                Duration.ofMillis(MINIMUM_ELECTION_TIMEOUT_MILLISECONDS),
                Snapshotter::new,
                true,
                new ProcessorManagerFactoryImpl(LONG_RUN_TEST ? SleepStrategies::threadSleep : SleepStrategies::yielding)
        );
    }

    protected TestInfrastructureFactory<Long> getInfrastructureFactory() {
        return NetworkSimulation.createDelayingReordering(
                allServers,
                PACKET_DROP_PROBABILITY, MINIMUM_MESSAGE_DELAY_MICROS, MAXIMUM_MESSAGE_DELAY_MICROS, TimeUnit.MICROSECONDS);
    }

    private Server<Long> createServerAndState(long id, Set<Long> serverIds) {
        try {
            PersistentState<Long> persistentState = FileBasedPersistentState.createOrOpen(LongIDSerializer.INSTANCE, stateDirectoryForServer(id), id);
            Server<Long> server = TimingWrappers.wrap(serverFactory.create(persistentState, serverIds, new DumbRegularIntervalSnapshotHeuristic()), WARNING_THRESHOLD_MILLIS);
            allServers.put(id, server);
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException("Error creating persistent state", e);
        }
    }

    private Path stateDirectoryForServer(long id) {
        try {
            final Path resolve = stateFileDirectory.resolve(String.valueOf(id));
            Files.createDirectories(resolve);
            return resolve;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterEach
    void tearDown() {
        Closeables.closeQuietly(testClusterClient, clusterAdminClient);
        stopServers();
        Closeables.closeQuietly(clusterFactory);
        ExecutorUtil.shutdownAndAwaitTermination(testExecutorService, 5, SECONDS);
        allServers.clear();
        System.gc();
    }

    @Test
    void willElectALeader() {
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);
    }

    @Test
    void willElectANewLeader_WhenTheExistingLeaderFails() {
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);
        Server<Long> oldLeader = getLeaderWithRetries();
        oldLeader.stop();
        await().atMost(10, SECONDS).until(this::aLeaderIsElected);
        oldLeader.start();
    }

    @Test
    void willProgressWithNoFailures() {
        try {
            countUp();
            waitForAllServersToCatchUp();
        } catch (Exception ex) {
            logStateAndFail(ex);
        }
    }

    @Test
    void willProgressWithFailures() throws InterruptedException, ExecutionException {
        Future<?> counterClientThread = testExecutorService.submit(() -> {
            try {
                countUp();
            } catch (Exception ex) {
                logStateAndFail(ex);
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
            } catch (Exception ex) {
                logStateAndFail(ex);
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
            } catch (Exception ex) {
                logStateAndFail(ex);
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

    @Test
    void willProgressWithDisruptiveServer() throws ExecutionException, InterruptedException {
        testExecutorService.submit(() -> {
            try {
                countUp(0, 10);
            } catch (Exception ex) {
                logStateAndFail(ex);
            }
        }).get();

        long newServerId = ALL_SERVER_IDS.size() + 1;
        try (final Server<Long> server = createServerAndState(newServerId, allServers.keySet())) {
            server.start();
            testExecutorService.submit(() -> {
                try {
                    countUp(10, COUNT_UP_TARGET);
                } catch (Exception ex) {
                    logStateAndFail(ex);
                }
            }).get();
        }

        Optional<Server<Long>> optionalLeader = getLeader();
        assertThat(optionalLeader).isPresent();
        Server<Long> leader = optionalLeader.get();
        assertThat(leader.getId()).isNotEqualTo(newServerId);
        assertThat(leader.getTerm().getNumber()).isLessThanOrEqualTo((int) Math.ceil(allServers.get(newServerId).getTerm().getNumber() * 0.1));
    }

    private void logStateAndFail(Exception e) {
        LOGGER.error("Failing due to exception", e);
        printThreadDump();
        fail(e);
    }

    private Runnable addOrRemoveAServer(AtomicLong newServerIdCounter) {
        return () -> {
            try {
                if (allServers.size() <= 3) {
                    addNewServer(newServerIdCounter.getAndIncrement());
                } else if (allServers.size() >= 8) {
                    removeRandomServer();
                } else {
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        addNewServer(newServerIdCounter.getAndIncrement());
                    } else {
                        removeRandomServer();
                    }
                }
            } catch (Exception e) {
                testFailure.set(new RuntimeException("Error occurred adding or removing a server", e));
            }
        };
    }

    private void addNewServer(long newServerId) {
        try {
            Set<Long> newServersView = new HashSet<>(allServers.keySet());
            newServersView.add(newServerId);
            final Server<Long> server = createServerAndState(newServerId, newServersView);

            LOGGER.info("Adding server {}, (new set={})", newServerId, newServersView);
            server.start();
            AddServerResponse<Long> response = clusterAdminClient.addNewServer(newServerId);
            switch (response.getStatus()) {
                case TIMEOUT:
                case NOT_LEADER:
                    LOGGER.error("Adding server failed, response: " + response);
                    server.close();
                    allServers.remove(newServerId);
                    break;
                case OK:
                    LOGGER.info("Server {} added response: {}", newServerId, response.getStatus());
                    break;
                default:
                    throw new IllegalStateException("Unexpected response: " + response);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void removeRandomServer() {
        try {
            Server<Long> server = chooseRandomServer();
            LOGGER.info("Removing server {}", server.getId());
            final RemoveServerResponse<Long> response = clusterAdminClient.removeServer(server.getId());
            if (response.getStatus() == OK) {
                LOGGER.info("Server {} remove succeeded, shutting down", server.getId());
                allServers.remove(server.getId());
                server.close();
                FileUtil.deleteRecursively(stateDirectoryForServer(server.getId()));
            } else {
                LOGGER.error("Server {} remove failed, aborting (status={})", server.getId(), response.getStatus());
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
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
        await().atMost(TIMEOUT_MINUTES, MINUTES).until(
                () -> allServers.values().stream().map(this::serverHasCaughtUp).reduce(true, (a, b) -> a && b)
        );
    }

    private boolean serverHasCaughtUp(Server<Long> server) {
        MonotonicCounter counter = (MonotonicCounter) server.getStateMachine();
        return counter.getCounter().intValue() == COUNT_UP_TARGET;
    }

    private void killThenResurrectCurrentLeader() {
        try {
            Server<Long> currentLeader = getLeaderWithRetries();
            Long killedServerId = currentLeader.getId();
            LOGGER.info("Killing server " + killedServerId);
            currentLeader.close();
            await().atMost(10, SECONDS).until(this::aLeaderIsElected);

            // Start a new node pointing to the same persistent state files
            Server<Long> newCurrentLeader = createServerAndState(killedServerId, ALL_SERVER_IDS);
            allServers.put(killedServerId, newCurrentLeader);
            newCurrentLeader.start();
            LOGGER.info("Server " + killedServerId + " restarted");
        } catch (Exception e) {
            testFailure.set(new RuntimeException("Killing leader failed!", e));
        }
    }

    private void triggerLeadershipTransfer() {
        try {
            Server<Long> currentLeader = getLeaderWithRetries();
            long currentLeaderId = currentLeader.getId();
            LOGGER.info("Telling server {} to transfer leadership", currentLeaderId);
            clusterAdminClient.deposeLeader();
            await().atMost(10, SECONDS).until(() -> this.serverIsNoLongerLeader(currentLeaderId));
        } catch (RuntimeException | ExecutionException | InterruptedException e) {
            testFailure.set(new RuntimeException("Error triggering leadership transfer", e));
        }
    }

    private boolean serverIsNoLongerLeader(long serverId) {
        return getLeader()
                .filter(server -> server.getId() != serverId)
                .isPresent();
    }

    private void countUp() throws Exception {
        countUp(0, COUNT_UP_TARGET);
    }

    private void countUp(int fromValue, int amountToAdd) throws Exception {
        try (MonotonicCounterClient counterClient = new MonotonicCounterClient(testClusterClient, BigInteger.valueOf(fromValue))) {
            counterClient.register();
            for (int i = 0; i < amountToAdd; i++) {
                counterClient.increment(this::checkFailed);
            }
        }
    }

    private void checkFailed() {
        final RuntimeException exception = testFailure.get();
        if (exception != null) {
            throw exception;
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

    private Server<Long> getLeaderWithRetries() {
        for (int i = 0; i < 5; i++) {
            final Optional<Server<Long>> leader = getLeader();
            if (leader.isPresent()) {
                return leader.get();
            } else {
                LOGGER.debug("Couldn't get leader, retrying...");
                pauseMillis(200);
            }
        }
        throw new IllegalStateException("Couldn't get current leader");
    }

    private void startServers() {
        allServers.values().forEach(Server::start);
    }

    private void stopServers() {
        allServers.values().forEach(server -> {
            try {
                server.close();
            } catch (NotRunningException e) {
                // This is fine, leader killer might have already done the job for us
            }
        });
    }

    private void printThreadDump() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);
        System.out.println(Arrays.stream(infos).map(Object::toString)
                .collect(Collectors.joining()));
    }
}
