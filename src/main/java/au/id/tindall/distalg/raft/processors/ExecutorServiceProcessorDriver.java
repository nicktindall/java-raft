package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.util.ThreadUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static au.id.tindall.distalg.raft.util.ExecutorUtil.shutdownAndAwaitTermination;
import static java.util.concurrent.TimeUnit.SECONDS;

class ExecutorServiceProcessorDriver<G extends Enum<G>> implements ProcessorDriver<G> {
    private static final int ES_SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final ExecutorService executorService;
    private final Supplier<SleepStrategy> sleepStrategySupplier;
    private final Map<G, ExecutorServiceProcessGroupDriver> drivers = new HashMap<>();

    public ExecutorServiceProcessorDriver() {
        this(Executors.newCachedThreadPool(), () -> () -> ThreadUtil.pauseMillis(1));
    }

    public ExecutorServiceProcessorDriver(ExecutorService executorService, Supplier<SleepStrategy> sleepStrategySupplier) {
        this.executorService = executorService;
        this.sleepStrategySupplier = sleepStrategySupplier;
    }

    @Override
    public void run(ProcessorGroup<G> processorGroup) {
        if (drivers.containsKey(processorGroup.getGroup())) {
            throw new IllegalStateException("Duplicate processor group: " + processorGroup.getGroup());
        }
        ExecutorServiceProcessGroupDriver task = new ExecutorServiceProcessGroupDriver(processorGroup, sleepStrategySupplier.get());
        drivers.put(processorGroup.getGroup(), task);
        executorService.submit(task);
    }

    @Override
    public void close() {
        drivers.values().forEach(ExecutorServiceProcessGroupDriver::stop);
        shutdownAndAwaitTermination(executorService, ES_SHUTDOWN_TIMEOUT_SECONDS, SECONDS);
    }
}
