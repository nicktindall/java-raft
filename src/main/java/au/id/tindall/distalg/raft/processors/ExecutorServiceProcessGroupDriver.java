package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.util.ExceptionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.concurrent.atomic.AtomicBoolean;

class ExecutorServiceProcessGroupDriver implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ProcessorGroup<?> processorGroup;
    private final SleepStrategy sleepStrategy;

    public ExecutorServiceProcessGroupDriver(ProcessorGroup<?> processorGroup, SleepStrategy sleepStrategy) {
        this.processorGroup = processorGroup;
        this.sleepStrategy = sleepStrategy;
    }

    @Override
    public void run() {
        try {
            running.set(true);
            ThreadContext.put("serverId", processorGroup.getServerID().toString());
            ThreadContext.put("processorGroup", processorGroup.getGroup().toString());
            LOGGER.debug("Starting processor for group {}", processorGroup.getGroup());
            while (running.get()) {
                Processor.ProcessResult result = processorGroup.runSingleIteration();
                if (result == Processor.ProcessResult.IDLE) {
                    sleepStrategy.sleep();
                }
            }
            LOGGER.debug("Processor for group {} ended gracefully", processorGroup.getGroup());
            processorGroup.finalise();
            ThreadContext.clearAll();
        } catch (Throwable t) {
            LOGGER.error("Exception thrown by group executor", t);
            ExceptionUtil.rethrowErrors(t);
        }
    }

    public void stop() {
        running.set(false);
    }
}
