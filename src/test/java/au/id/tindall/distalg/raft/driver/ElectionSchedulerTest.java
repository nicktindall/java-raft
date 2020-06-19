package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.Server;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ElectionSchedulerTest {

    private static final long TIMEOUT_MILLIS = 12345L;
    @Mock
    private Server<Long> server;
    @Mock
    private ElectionTimeoutGenerator electionTimeoutGenerator;
    @Mock
    private ScheduledExecutorService scheduledExecutorService;
    @Mock
    private ScheduledFuture timeoutFuture;

    private ElectionScheduler<Long> electionScheduler;

    @BeforeEach
    void setUp() {
        electionScheduler = new ElectionScheduler<>(electionTimeoutGenerator, scheduledExecutorService);
        electionScheduler.setServer(server);
    }

    @Nested
    class StartElectionTimeouts {

        @BeforeEach
        void setUp() {
            when(electionTimeoutGenerator.next()).thenReturn(TIMEOUT_MILLIS);
            electionScheduler.startTimeouts();
        }

        @Test
        void willScheduleAnElectionTimeout() {
            verify(scheduledExecutorService).schedule(any(Runnable.class), eq(TIMEOUT_MILLIS), eq(MILLISECONDS));
        }

        @Test
        void willThrowIfTimeoutsAlreadyStarted() {
            assertThatThrownBy(() -> electionScheduler.startTimeouts()).isInstanceOf(IllegalStateException.class);
            verify(scheduledExecutorService).schedule(any(Runnable.class), eq(TIMEOUT_MILLIS), eq(MILLISECONDS));
        }
    }

    @Nested
    class ResetElectionTimeout {

        @Test
        @SuppressWarnings("unchecked")
        void willCancelExistingTimeoutThenScheduleANewOne() {
            when(electionTimeoutGenerator.next()).thenReturn(TIMEOUT_MILLIS);
            when(scheduledExecutorService.schedule(any(Runnable.class), eq(TIMEOUT_MILLIS), eq(MILLISECONDS)))
                    .thenReturn(timeoutFuture);

            electionScheduler.startTimeouts();
            reset(scheduledExecutorService);

            electionScheduler.resetTimeout();
            InOrder sequence = inOrder(timeoutFuture, scheduledExecutorService);
            sequence.verify(timeoutFuture).cancel(false);
            sequence.verify(scheduledExecutorService).schedule(any(Runnable.class), eq(TIMEOUT_MILLIS), eq(MILLISECONDS));
        }

        @Test
        void willThrowIfTimeoutsAreNotStarted() {
            assertThatThrownBy(() -> electionScheduler.resetTimeout()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class StopElectionTimeouts {

        @Test
        @SuppressWarnings("unchecked")
        void willCancelOutstandingTimeout() {
            when(electionTimeoutGenerator.next()).thenReturn(TIMEOUT_MILLIS);
            when(scheduledExecutorService.schedule(any(Runnable.class), eq(TIMEOUT_MILLIS), eq(MILLISECONDS)))
                    .thenReturn(timeoutFuture);
            electionScheduler.startTimeouts();

            electionScheduler.stopTimeouts();
            verify(timeoutFuture).cancel(false);
        }


        @Test
        void willThrowWhenTimeoutsAreNotStarted() {
            assertThatThrownBy(() -> electionScheduler.stopTimeouts()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class ElectionTimeout {

        private Runnable timeoutFunction;

        @BeforeEach
        void setUp() {
            when(electionTimeoutGenerator.next()).thenReturn(TIMEOUT_MILLIS);
            when(scheduledExecutorService.schedule(any(Runnable.class), eq(TIMEOUT_MILLIS), eq(MILLISECONDS)))
                    .thenReturn(timeoutFuture);
            electionScheduler.startTimeouts();
            ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
            verify(scheduledExecutorService).schedule(captor.capture(), eq(TIMEOUT_MILLIS), eq(MILLISECONDS));
            timeoutFunction = captor.getValue();
            reset(scheduledExecutorService);
        }

        @Test
        void willCallElectionTimeoutOnTheServer() {
            timeoutFunction.run();
            verify(server).electionTimeout();
        }

        @Test
        void willScheduleNextTimeout() {
            timeoutFunction.run();
            verify(scheduledExecutorService).schedule(any(Runnable.class), eq(TIMEOUT_MILLIS), eq(MILLISECONDS));
        }
    }
}