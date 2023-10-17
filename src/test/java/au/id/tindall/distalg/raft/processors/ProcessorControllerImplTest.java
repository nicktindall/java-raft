package au.id.tindall.distalg.raft.processors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;

import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProcessorControllerImplTest {

    @Mock
    private Processor<RaftProcessorGroup> processor;
    private ProcessorControllerImpl processorController;

    @BeforeEach
    void setUp() {
        processorController = new ProcessorControllerImpl(processor);
    }

    @Nested
    class Stop {

        @Test
        void willTransitionToStopRequestedState() {
            processorController.stop();
            assertThat(processorController.state()).isEqualTo(ProcessorState.STOP_REQUESTED);
            assertThat(processorController.process()).isEqualTo(Processor.ProcessResult.FINISHED);
            verify(processor, never()).process();
            assertThat(processorController.state()).isEqualTo(ProcessorState.STOPPED);
        }

        @Test
        void isIdempotent() {
            processorController.stop();
            processorController.stop();
            assertThat(processorController.state()).isEqualTo(ProcessorState.STOP_REQUESTED);
        }
    }

    @Nested
    class StopAndWait {

        @Test
        void willBlockTheCallingThreadUntilProcessorIsStopped() {
            Thread t = new Thread(() -> processorController.stopAndWait());
            t.start();
            while (processorController.state() != ProcessorState.STOP_REQUESTED) {
                pauseMillis(1);
            }
            pauseMillis(10);
            assertThat(t.isAlive()).isTrue();
            assertThat(processorController.process()).isEqualTo(Processor.ProcessResult.FINISHED);
            verify(processor, never()).process();
            assertThat(processorController.state()).isEqualTo(ProcessorState.STOPPED);
            await().atMost(1, TimeUnit.SECONDS).until(() -> !t.isAlive());
        }

        @Test
        void willBlockSubsequentStoppersUntilStopped() {
            Thread t1 = new Thread(() -> processorController.stopAndWait());
            t1.start();
            Thread t2 = new Thread(() -> processorController.stopAndWait());
            t2.start();
            while (processorController.state() != ProcessorState.STOP_REQUESTED) {
                pauseMillis(1);
            }
            pauseMillis(10);
            assertThat(t1.isAlive()).isTrue();
            assertThat(t2.isAlive()).isTrue();
            assertThat(processorController.process()).isEqualTo(Processor.ProcessResult.FINISHED);
            verify(processor, never()).process();
            assertThat(processorController.state()).isEqualTo(ProcessorState.STOPPED);
            await().atMost(1, TimeUnit.SECONDS).until(() -> !t1.isAlive());
            await().atMost(1, TimeUnit.SECONDS).until(() -> !t2.isAlive());
        }
    }

    @Nested
    class ProcessDelegation {

        @Test
        void processWillDelegateToProcessorWhenRunning() {
            processorController.process();

            verify(processor).process();
        }

        @Test
        void beforeFirstWillDelegateToProcessor() {
            processorController.beforeFirst();

            verify(processor).beforeFirst();
        }

        @Test
        void afterLastWillDelegateToProcessor() {
            processorController.afterLast();

            verify(processor).afterLast();
        }
    }
}