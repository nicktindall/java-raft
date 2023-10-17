package au.id.tindall.distalg.raft.processors;

import org.apache.logging.log4j.ThreadContext;

class ManualProcessorGroupDriver {

    private final ProcessorGroup<?> processorGroup;

    ManualProcessorGroupDriver(ProcessorGroup<?> processorGroup) {
        this.processorGroup = processorGroup;
    }

    public boolean flush() {
        ThreadContext.put("serverId", String.valueOf(processorGroup.getServerID()));
        ThreadContext.put("processorGroup", processorGroup.getGroup().toString());
        boolean didSomething = false;
        try {
            while (processorGroup.runSingleIteration() != Processor.ProcessResult.IDLE) {
                didSomething = true;
            }
        } finally {
            ThreadContext.clearAll();
        }

        return didSomething;
    }
}
