package au.id.tindall.distalg.raft.processors;

import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.logging.log4j.LogManager.getLogger;

class ProcessorGroupImpl<G extends Enum<G>> implements ProcessorGroup<G> {

    private static final Logger LOGGER = getLogger();

    private final Serializable serverID;
    private final G group;
    private final List<Processor<G>> processors;
    private final Queue<Processor<G>> newProcessors;

    public ProcessorGroupImpl(G group, Serializable serverID) {
        this.serverID = serverID;
        this.group = group;
        this.processors = new ArrayList<>();
        this.newProcessors = new ConcurrentLinkedQueue<>();
    }

    @Override
    public Processor.ProcessResult runSingleIteration() {
        addAllNewProcessors();
        Processor.ProcessResult iterationResult = Processor.ProcessResult.IDLE;
        for (int i = 0; i < processors.size(); i++) {
            try {
                final Processor<G> processor = processors.get(i);
                final Processor.ProcessResult result = processor.process();
                if (result == Processor.ProcessResult.FINISHED) {
                    LOGGER.debug("{} processor completed", processor.getName());
                    processors.remove(i).afterLast();
                    i--;
                } else if (result == Processor.ProcessResult.BUSY) {
                    iterationResult = result;
                }
            } catch (RuntimeException e) {
                LOGGER.error("Error running processor", e);
            }
        }
        return iterationResult;
    }

    @Override
    public void finalise() {
        processors.forEach(Processor::afterLast);
        processors.clear();
    }

    private void addAllNewProcessors() {
        while (true) {
            Processor<G> newProcessor = newProcessors.poll();
            if (newProcessor != null) {
                processors.add(newProcessor);
                newProcessor.beforeFirst();
            } else {
                return;
            }
        }
    }

    @Override
    public void add(Processor<G> processor) {
        newProcessors.add(processor);
    }

    @Override
    public Serializable getServerID() {
        return serverID;
    }

    @Override
    public G getGroup() {
        return group;
    }
}
