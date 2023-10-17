package au.id.tindall.distalg.raft.processors;

import java.util.HashMap;
import java.util.Map;

public class ManualProcessorDriver<G extends Enum<G>> implements ProcessorDriver<G> {

    private final Map<G, ManualProcessorGroupDriver> groupExecutors = new HashMap<>();

    @Override
    public void run(ProcessorGroup<G> processorGroup) {
        if (groupExecutors.containsKey(processorGroup.getGroup())) {
            throw new IllegalStateException("Got duplicate processor group " + processorGroup.getGroup());
        }
        groupExecutors.put(processorGroup.getGroup(), new ManualProcessorGroupDriver(processorGroup));
    }

    public boolean flush() {
        boolean somethingHappened = false;
        while (flushAllGroupExecutors()) {
            somethingHappened = true;
        }
        return somethingHappened;
    }

    private boolean flushAllGroupExecutors() {
        return groupExecutors.values().stream().map(ManualProcessorGroupDriver::flush).reduce(false, (state, next) -> state || next);
    }
}
