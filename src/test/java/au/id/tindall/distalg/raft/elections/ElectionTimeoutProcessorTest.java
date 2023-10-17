package au.id.tindall.distalg.raft.elections;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.processors.Processor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ElectionTimeoutProcessorTest {

    @Mock
    private Server<Integer> server;

    @Nested
    class WhenTimeoutIsDue {

        @BeforeEach
        void setUp() {
            when(server.timeoutNowIfDue()).thenReturn(true);
        }

        @Test
        void willTimeoutAndReturnBusyIfDue() {
            ElectionTimeoutProcessor<Integer> processor = new ElectionTimeoutProcessor<>(server);
            assertThat(processor.process()).isEqualTo(Processor.ProcessResult.BUSY);
            verify(server).timeoutNowIfDue();
        }
    }

    @Nested
    class WhenTimeoutIsNotDue {

        @BeforeEach
        void setUp() {
            when(server.timeoutNowIfDue()).thenReturn(false);
        }

        @Test
        void willTimeoutAndReturnBusyIfDue() {
            ElectionTimeoutProcessor<Integer> processor = new ElectionTimeoutProcessor<>(server);
            assertThat(processor.process()).isEqualTo(Processor.ProcessResult.IDLE);
            verify(server).timeoutNowIfDue();
        }
    }
}