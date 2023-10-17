package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.threading.NamedThreadFactory;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class ProcessorManagerFactoryImpl implements ProcessorManagerFactory {

    private final Supplier<SleepStrategy> sleepStrategySupplier;

    public ProcessorManagerFactoryImpl(Supplier<SleepStrategy> sleepStrategySupplier) {
        this.sleepStrategySupplier = sleepStrategySupplier;
    }

    public ProcessorManagerFactoryImpl() {
        this(SleepStrategies::yielding);
    }

    @Override
    public ProcessorManager<RaftProcessorGroup> create(Serializable serverID) {
        ExecutorServiceProcessorDriver<RaftProcessorGroup> processorDriver = new ExecutorServiceProcessorDriver<>(
                Executors.newFixedThreadPool(RaftProcessorGroup.values().length,
                        NamedThreadFactory.forThreadGroup(serverID + "-processor")), sleepStrategySupplier);
        return new ProcessorManagerImpl<>(serverID, processorDriver);
    }
}
