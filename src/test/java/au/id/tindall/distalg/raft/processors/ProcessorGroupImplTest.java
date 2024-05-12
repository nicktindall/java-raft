package au.id.tindall.distalg.raft.processors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static au.id.tindall.distalg.raft.processors.Processor.ProcessResult.BUSY;
import static au.id.tindall.distalg.raft.processors.Processor.ProcessResult.FINISHED;
import static au.id.tindall.distalg.raft.processors.Processor.ProcessResult.IDLE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProcessorGroupImplTest {

    @Mock
    private Processor<Group> throwingOnProcessProcessor;
    @Mock
    private Processor<Group> throwingOnAfterLastProcessor;
    @Mock
    private Processor<Group> throwingOnBeforeFirstProcessor;
    @Mock
    private Processor<Group> nonThrowingProcessor;

    private ProcessorGroupImpl<Group> processorGroup;

    @BeforeEach
    void setUp() {
        processorGroup = new ProcessorGroupImpl<>(Group.ONLY_ONE, 123);
        lenient().when(nonThrowingProcessor.process()).thenReturn(BUSY);
        lenient().when(nonThrowingProcessor.getName()).thenReturn("NonThrowingProcessor");

        lenient().when(throwingOnProcessProcessor.process()).thenThrow(IllegalStateException.class);
        lenient().when(throwingOnProcessProcessor.getName()).thenReturn("ThrowingOnProcessProcessor");

        lenient().when(throwingOnAfterLastProcessor.process()).thenReturn(FINISHED);
        lenient().doThrow(IllegalStateException.class).when(throwingOnAfterLastProcessor).afterLast();
        lenient().when(throwingOnAfterLastProcessor.getName()).thenReturn("ThrowingOnAfterLastProcessor");

        lenient().doThrow(IllegalStateException.class).when(throwingOnBeforeFirstProcessor).beforeFirst();
        lenient().when(throwingOnBeforeFirstProcessor.getName()).thenReturn("ThrowingOnBeforeFirstProcessor");
    }

    @Test
    void exceptionsAreCaughtWhenThrownInProcess() {
        processorGroup.add(throwingOnProcessProcessor);
        processorGroup.add(nonThrowingProcessor);
        assertThat(processorGroup.runSingleIteration()).isEqualTo(BUSY);
        assertThat(processorGroup.runSingleIteration()).isEqualTo(BUSY);
        verify(nonThrowingProcessor, times(2)).process();
        verify(throwingOnProcessProcessor, times(1)).process();
    }

    @Test
    void exceptionsAreCaughtWhenThrownInAfterLast_OnCompletion() {
        processorGroup.add(throwingOnAfterLastProcessor);
        processorGroup.add(nonThrowingProcessor);
        assertThat(processorGroup.runSingleIteration()).isEqualTo(BUSY);
        assertThat(processorGroup.runSingleIteration()).isEqualTo(BUSY);
        verify(nonThrowingProcessor, times(2)).process();
        verify(throwingOnAfterLastProcessor, times(1)).process();
        verify(throwingOnAfterLastProcessor, times(1)).afterLast();
    }

    @Test
    void exceptionsAreCaughtWhenThrownInAfterLast_OnFinalise() {
        lenient().when(throwingOnAfterLastProcessor.process()).thenReturn(IDLE);
        processorGroup.add(throwingOnAfterLastProcessor);
        processorGroup.add(nonThrowingProcessor);
        processorGroup.runSingleIteration();
        processorGroup.finalise();
        verify(nonThrowingProcessor, times(1)).afterLast();
        verify(throwingOnAfterLastProcessor, times(1)).afterLast();
    }

    @Test
    void exceptionsAreCaughtWhenThrownInBeforeFirst() {
        processorGroup.add(throwingOnBeforeFirstProcessor);
        processorGroup.add(nonThrowingProcessor);
        processorGroup.runSingleIteration();
        verify(nonThrowingProcessor, times(1)).process();
        verify(throwingOnBeforeFirstProcessor, times(0)).process();
    }

    private enum Group {
        ONLY_ONE
    }
}