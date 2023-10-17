package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.util.Closeables;

import java.io.Closeable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ProcessorManagerImpl<G extends Enum<G>> implements ProcessorManager<G>, Closeable {

    private final Serializable serverID;
    private final ProcessorDriver<G> processorDriver;
    private final Map<G, ProcessorGroupImpl<G>> processorGroups;

    public ProcessorManagerImpl(Serializable serverID) {
        this(serverID, new ExecutorServiceProcessorDriver<>());
    }

    public ProcessorManagerImpl(Serializable serverID, ProcessorDriver<G> processorDriver) {
        this.serverID = serverID;
        this.processorGroups = new HashMap<>();
        this.processorDriver = processorDriver;
    }

    @Override
    public ProcessorController runProcessor(Processor<G> processor) {
        final ProcessorGroupImpl<G> runner = processorGroups.computeIfAbsent(processor.getGroup(), pg -> {
            ProcessorGroupImpl<G> newGroup = new ProcessorGroupImpl<>(pg, serverID);
            processorDriver.run(newGroup);
            return newGroup;
        });
        ProcessorControllerImpl<G> pc = new ProcessorControllerImpl<>(processor);
        runner.add(pc);
        return pc;
    }

    @Override
    public void close() {
        Closeables.closeQuietly(processorDriver);
    }
}
