package au.id.tindall.distalg.raft.processors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static au.id.tindall.distalg.raft.util.ExecutorUtil.shutdownAndAwaitTermination;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutorServiceProcessGroupDriverTest {

    @Mock
    private ProcessorGroup<RaftProcessorGroup> processorRunner;

    @BeforeEach
    void setUp() {
        when(processorRunner.getServerID()).thenReturn(123);
        when(processorRunner.getGroup()).thenReturn(RaftProcessorGroup.LOG_PROCESSING);
    }

    @Test
    void willCallProcessOneIterationRepeatedlyWhileRunningThenFinalise() throws ExecutionException, InterruptedException {
        AtomicInteger runCounts = new AtomicInteger();
        doAnswer(iom -> {
            runCounts.incrementAndGet();
            return Processor.ProcessResult.IDLE;
        }).when(processorRunner).runSingleIteration();
        ExecutorServiceProcessGroupDriver groupExecutor = new ExecutorServiceProcessGroupDriver(processorRunner, SleepStrategies.yielding());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            verify(processorRunner, never()).runSingleIteration();
            Future<?> submit = executorService.submit(groupExecutor);
            await().atMost(2, SECONDS).until(() -> runCounts.get() > 0);
            groupExecutor.stop();
            submit.get();
            verify(processorRunner).finalise();
        } finally {
            shutdownAndAwaitTermination(executorService, 1, SECONDS);
        }
    }
}