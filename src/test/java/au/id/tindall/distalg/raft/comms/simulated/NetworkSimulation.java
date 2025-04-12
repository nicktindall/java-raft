package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.comms.TestInfrastructureFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface NetworkSimulation<I> extends TestInfrastructureFactory<I> {

    static <I> NetworkSimulation<I> createInstant(Map<I, Server<I>> servers) {
        return new SimulatedTestInfrastructureFactory<>(new InstantRouter<>(), servers);
    }

    static <I> NetworkSimulation<I> createDelayingReordering(Map<I, Server<I>> servers, float packetDropProbability, long minimumDelay, long maximumDelay, TimeUnit delayUnit) {
        SimulatedTestInfrastructureFactory<I> factory = new SimulatedTestInfrastructureFactory<>(new DelayingReorderingRouter<>(packetDropProbability, minimumDelay, maximumDelay, delayUnit), servers);
        factory.startNetworkSimulator();
        return factory;
    }

    boolean flush();
}
