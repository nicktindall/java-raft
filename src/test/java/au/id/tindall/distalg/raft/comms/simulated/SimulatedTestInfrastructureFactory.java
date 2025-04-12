package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.clusterclient.ClusterClient;
import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.processors.SleepStrategies;
import au.id.tindall.distalg.raft.threading.NamedThreadFactory;
import au.id.tindall.distalg.raft.util.Closeables;
import au.id.tindall.distalg.raft.util.ExecutorUtil;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class SimulatedTestInfrastructureFactory<I> implements NetworkSimulation<I> {

    private final MessageStats messageStats;
    private final NetworkSimulator<I> networkSimulator;
    private final Map<I, Server<I>> allServers;
    private ExecutorService executor;

    SimulatedTestInfrastructureFactory(Router<I> router, Map<I, Server<I>> allServers) {
        this.networkSimulator = new NetworkSimulator<>(router, SleepStrategies.yielding());
        this.allServers = allServers;
        this.messageStats = new MessageStats();
    }

    @Override
    public Cluster<I> createCluster(I serverId) {
        return networkSimulator.createCluster(serverId, messageStats::recordMessageSent);
    }

    public boolean flush() {
        return networkSimulator.flush();
    }

    public Future<?> startNetworkSimulator() {
        executor = Executors.newSingleThreadExecutor(NamedThreadFactory.forSingleThread("network-simulator"));
        return executor.submit(networkSimulator);
    }

    public void close() {
        messageStats.logStats();
        Closeables.closeQuietly(networkSimulator);
        ExecutorUtil.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }

    @Override
    public ClusterClient<I> createClusterClient() {
        return new TestClusterClient<>(allServers);
    }
}
