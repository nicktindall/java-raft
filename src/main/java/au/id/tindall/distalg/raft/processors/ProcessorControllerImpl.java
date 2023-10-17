package au.id.tindall.distalg.raft.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMillis;

class ProcessorControllerImpl<G extends Enum<G>> implements ProcessorController, Processor<G> {
    private static final Logger LOGGER = LogManager.getLogger();

    private final AtomicReference<ProcessorState> state = new AtomicReference<>(ProcessorState.RUNNING);
    private final Processor<G> processor;

    public ProcessorControllerImpl(Processor<G> processor) {
        this.processor = processor;
    }

    @Override
    public final void stop() {
        if (!state.compareAndSet(ProcessorState.RUNNING, ProcessorState.STOP_REQUESTED)) {
            logCouldNotStop();
        }
    }

    @Override
    public void stopAndWait() {
        if (!state.compareAndSet(ProcessorState.RUNNING, ProcessorState.STOP_REQUESTED)) {
            logCouldNotStop();
        }
        while (state.get() != ProcessorState.STOPPED) {
            pauseMillis(1);
        }
    }

    @Override
    public Processor.ProcessResult process() {
        if (state.compareAndSet(ProcessorState.STOP_REQUESTED, ProcessorState.STOPPED)) {
            return Processor.ProcessResult.FINISHED;
        }
        return processor.process();
    }

    @Override
    public G getGroup() {
        return processor.getGroup();
    }

    @Override
    public String getName() {
        return processor.getName();
    }

    @Override
    public void afterLast() {
        processor.afterLast();
    }

    @Override
    public void beforeFirst() {
        processor.beforeFirst();
    }

    @Override
    public ProcessorState state() {
        return state.get();
    }

    private void logCouldNotStop() {
        LOGGER.warn("Couldn't stop processor, was in state {} (processor={})", state.get(), processor);
    }
}
