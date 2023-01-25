package au.id.tindall.distalg.raft.elections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class ElectionSchedulerTest {

    private static final long TIMEOUT_MILLIS = 100L;
    private static final Instant CURRENT_TIME = Instant.now();
    @Mock
    private ElectionTimeoutGenerator electionTimeoutGenerator;

    private final AtomicReference<Instant> currentTime = new AtomicReference<>(CURRENT_TIME);
    private ElectionScheduler electionScheduler;

    @BeforeEach
    void setUp() {
        electionScheduler = new ElectionScheduler(electionTimeoutGenerator, currentTime::get);
        lenient().when(electionTimeoutGenerator.next()).thenReturn(TIMEOUT_MILLIS);
    }

    @Nested
    class StartElectionTimeouts {

        @BeforeEach
        void setUp() {
            electionScheduler.startTimeouts();
        }

        @AfterEach
        void tearDown() {
            electionScheduler.stopTimeouts();
        }

        @Test
        void willScheduleAnElectionTimeout() {
            currentTime.set(currentTime.get().plusMillis(TIMEOUT_MILLIS - 1));
            assertThat(electionScheduler.shouldTimeout()).isFalse();
            currentTime.set(currentTime.get().plusMillis(2));
            assertThat(electionScheduler.shouldTimeout()).isTrue();
        }

        @Test
        void willThrowIfTimeoutsAlreadyStarted() {
            assertThatThrownBy(() -> electionScheduler.startTimeouts()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class ResetElectionTimeout {

        @Test
        void willPreventTimeoutOccurring() {
            electionScheduler.startTimeouts();
            currentTime.set(currentTime.get().plusMillis(TIMEOUT_MILLIS));
            assertThat(electionScheduler.shouldTimeout()).isTrue();
            electionScheduler.resetTimeout();
            assertThat(electionScheduler.shouldTimeout()).isFalse();
        }

        @Test
        void willThrowIfTimeoutsAreNotStarted() {
            assertThatThrownBy(() -> electionScheduler.resetTimeout()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class StopElectionTimeouts {

        @Test
        void willCancelOutstandingTimeout() {
            electionScheduler.startTimeouts();
            currentTime.set(currentTime.get().plusMillis(TIMEOUT_MILLIS));
            assertThat(electionScheduler.shouldTimeout()).isTrue();
            electionScheduler.stopTimeouts();
            assertThat(electionScheduler.shouldTimeout()).isFalse();
        }

        @Test
        void willThrowWhenTimeoutsAreNotStarted() {
            assertThatThrownBy(() -> electionScheduler.stopTimeouts()).isInstanceOf(IllegalStateException.class);
        }
    }
}