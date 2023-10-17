package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.processors.Processor;
import au.id.tindall.distalg.raft.processors.ProcessorController;
import au.id.tindall.distalg.raft.processors.ProcessorManager;
import au.id.tindall.distalg.raft.processors.RaftProcessorGroup;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TestProcessorManager implements ProcessorManager<RaftProcessorGroup> {

    private final Map<Class<? extends Processor<RaftProcessorGroup>>, ProcessorAndController> addedProcessors = new HashMap<>();
    private boolean closed = false;

    @SuppressWarnings("unchecked")
    @Override
    public ProcessorController runProcessor(Processor<RaftProcessorGroup> processor) {
        if (addedProcessors.containsKey(processor.getClass())) {
            throw new IllegalStateException("Multiple processors added of same type, need to use a different data structure");
        }
        ProcessorAndController pAndC = new ProcessorAndController(processor);
        processor.beforeFirst();
        addedProcessors.put((Class<? extends Processor<RaftProcessorGroup>>) processor.getClass(), pAndC);
        return pAndC.getProcessorController();
    }

    @SuppressWarnings("unchecked")
    public <T extends Processor<RaftProcessorGroup>> T getProcessor(Class<T> processorClass) {
        return (T) addedProcessors.get(processorClass);
    }

    public <T extends Processor<RaftProcessorGroup>> ProcessorController getProcessorController(Class<T> processorClass) {
        return addedProcessors.get(processorClass).getProcessorController();
    }

    private static class ProcessorAndController {
        private final ProcessorController processorController;
        private final Processor<RaftProcessorGroup> processor;

        private ProcessorAndController(Processor<RaftProcessorGroup> processor) {
            this.processorController = mock(ProcessorController.class);
            this.processor = processor;
        }

        public Processor<RaftProcessorGroup> getProcessor() {
            return processor;
        }

        public ProcessorController getProcessorController() {
            return processorController;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
