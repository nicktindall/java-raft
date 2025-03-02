package au.id.tindall.distalg.raft.processors;

import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

import static org.apache.logging.log4j.LogManager.getLogger;

class ProcessorGroupImpl<G extends Enum<G>> implements ProcessorGroup<G> {

    private static final Logger LOGGER = getLogger();

    private final Object serverID;
    private final G group;
    private final List<Processor<G>> processors;
    private final Queue<Processor<G>> newProcessors;

    public ProcessorGroupImpl(G group, Object serverID) {
        this.serverID = serverID;
        this.group = group;
        this.processors = new ArrayList<>();
        this.newProcessors = new ConcurrentLinkedQueue<>();
    }

    @Override
    public Processor.ProcessResult runSingleIteration() {
        addAllNewProcessors();
        Processor.ProcessResult iterationResult = Processor.ProcessResult.IDLE;
        int index = 0;
        while (index < processors.size()) {
            Processor.ProcessResult processResult = runSingleProcessor(index);
            switch (processResult) {
                case FINISHED:
                    safelyRemoveProcessor(index);
                    break;
                case BUSY:
                    iterationResult = Processor.ProcessResult.BUSY;
                    // fall through
                case IDLE:
                    index++;
                    break;
                default:
                    throw new IllegalStateException("Unexpected result: " + processResult);
            }
        }
        return iterationResult;
    }

    private Processor.ProcessResult runSingleProcessor(int index) {
        try {
            final Processor<G> processor = processors.get(index);
            final Processor.ProcessResult result = processor.process();
            if (result == Processor.ProcessResult.FINISHED) {
                LOGGER.debug("{} processor completed", processor.getName());
            }
            return result;
        } catch (RuntimeException e) {
            LOGGER.error("Error running processor: {}", processors.get(index).getName(), e);
            return Processor.ProcessResult.FINISHED;
        }
    }

    private void safelyRemoveProcessor(int index) {
        Processor<G> removed = processors.remove(index);
        try {
            removed.afterLast();
        } catch (RuntimeException e) {
            LOGGER.error("Error removing processor: {}", removed.getName(), e);
        }
    }

    @Override
    public void finalise() {
        IntStream.range(0, processors.size())
                .forEach(i -> safelyRemoveProcessor(0));
        assert processors.isEmpty() : "Processors should be empty";
    }

    private void addAllNewProcessors() {
        while (true) {
            Processor<G> newProcessor = newProcessors.poll();
            if (newProcessor != null) {
                try {
                    newProcessor.beforeFirst();
                    processors.add(newProcessor);
                } catch (RuntimeException e) {
                    LOGGER.error("Error adding new processor: {}", newProcessor.getName(), e);
                }
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
    public Object getServerID() {
        return serverID;
    }

    @Override
    public G getGroup() {
        return group;
    }
}
