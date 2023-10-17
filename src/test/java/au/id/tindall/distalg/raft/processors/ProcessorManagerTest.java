package au.id.tindall.distalg.raft.processors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ProcessorManagerTest {

    private static final long TIMEOUT_MS = 1_000;

    @Test
    void testProcessorLifecycle() {
        try (final ProcessorManagerImpl<TestProcessorGroup> manager = new ProcessorManagerImpl<>(123)) {
            TestProcessor tp = new TestProcessor();
            manager.runProcessor(tp);
            tp.waitForBeforeFirstToHaveBeenCalled();
            tp.waitForProcessToHaveBeenCalled();
            tp.stop();
            tp.waitForAfterLastToHaveBeenCalled();
        }
    }

    @Test
    void testProcessorControl_Stop() {
        try (final ProcessorManagerImpl<TestProcessorGroup> manager = new ProcessorManagerImpl<>(123)) {
            TestProcessor tp = new TestProcessor();
            ProcessorController processorController = manager.runProcessor(tp);
            tp.waitForProcessToHaveBeenCalled();
            processorController.stop();
            tp.waitForAfterLastToHaveBeenCalled();
        }
    }

    @Test
    void testProcessorControl_StopAndWait() {
        try (final ProcessorManagerImpl<TestProcessorGroup> manager = new ProcessorManagerImpl<>(123)) {
            TestProcessor tp = new TestProcessor();
            ProcessorController processorController = manager.runProcessor(tp);
            tp.waitForProcessToHaveBeenCalled();
            processorController.stopAndWait();
            tp.waitForAfterLastToHaveBeenCalled();
        }
    }

    @Test
    void testProcessorControl_Status() {
        try (final ProcessorManagerImpl<TestProcessorGroup> manager = new ProcessorManagerImpl<>(123)) {
            TestProcessor tp = new TestProcessor();
            ProcessorController processorController = manager.runProcessor(tp);
            tp.waitForProcessToHaveBeenCalled();
            assertEquals(ProcessorState.RUNNING, processorController.state());
            processorController.stopAndWait();
            assertEquals(ProcessorState.STOPPED, processorController.state());
            tp.waitForAfterLastToHaveBeenCalled();
        }
    }

    private static class TestProcessor implements Processor<TestProcessorGroup> {

        private volatile boolean beforeFirstCalled;
        private volatile boolean afterLastCalled;
        private volatile boolean processCalled;
        private volatile boolean stopped;

        public void stop() {
            stopped = true;
        }

        @Override
        public void beforeFirst() {
            beforeFirstCalled = true;
        }

        @Override
        public void afterLast() {
            afterLastCalled = true;
        }

        @Override
        public ProcessResult process() {
            processCalled = true;
            if (stopped) {
                return ProcessResult.FINISHED;
            }
            return ProcessResult.IDLE;
        }

        @Override
        public TestProcessorGroup getGroup() {
            return TestProcessorGroup.SLOW;
        }

        public void waitForProcessToHaveBeenCalled() {
            long startTime = System.currentTimeMillis();
            while (!processCalled) {
                if (System.currentTimeMillis() - startTime > TIMEOUT_MS) {
                    fail("Process not called");
                }
            }
        }

        public void waitForBeforeFirstToHaveBeenCalled() {
            long startTime = System.currentTimeMillis();
            while (!beforeFirstCalled) {
                if (System.currentTimeMillis() - startTime > TIMEOUT_MS) {
                    fail("BeforeFirst not called");
                }
            }
        }

        public void waitForAfterLastToHaveBeenCalled() {
            long startTime = System.currentTimeMillis();
            while (!afterLastCalled) {
                if (System.currentTimeMillis() - startTime > TIMEOUT_MS) {
                    fail("AfterLast not called");
                }
            }
        }
    }

    enum TestProcessorGroup {
        FAST,
        SLOW
    }
}