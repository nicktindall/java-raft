package au.id.tindall.distalg.raft.elections;

import au.id.tindall.distalg.raft.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class ElectionSchedulerTest {

    private static final long TIMEOUT_MILLIS = 100L;
    private static final long SERVER_ID = 567L;
    @Mock
    private Server<Long> server;
    @Mock
    private ElectionTimeoutGenerator electionTimeoutGenerator;
    private AtomicBoolean timeoutOccurred;

    private ElectionScheduler<Long> electionScheduler;

    @BeforeEach
    void setUp() {
        timeoutOccurred = new AtomicBoolean(false);
        lenient().when(server.getId()).thenReturn(SERVER_ID);
        lenient().doAnswer(iom -> {
            timeoutOccurred.set(true);
            return null;
        }).when(server).electionTimeout();
        lenient().when(electionTimeoutGenerator.next()).thenReturn(TIMEOUT_MILLIS);
        electionScheduler = new ElectionScheduler<>(SERVER_ID, electionTimeoutGenerator, Instant::now);
        electionScheduler.setServer(server);
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
            await().atMost(TIMEOUT_MILLIS * 5, MILLISECONDS).until(() -> timeoutOccurred.get());
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
            long endTime = System.currentTimeMillis() + 500;
            while (System.currentTimeMillis() < endTime) {
                electionScheduler.resetTimeout();
                pauseMillis(TIMEOUT_MILLIS - 20);
            }
            assertThat(timeoutOccurred.get()).isFalse();
            await().atMost(TIMEOUT_MILLIS * 2, MILLISECONDS).until(() -> timeoutOccurred.get());
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
            IntStream.range(0, 50).forEach((i) -> {
                electionScheduler.startTimeouts();
                pauseMillis(ThreadLocalRandom.current().nextInt((int) TIMEOUT_MILLIS / 2, (int) TIMEOUT_MILLIS * 2));
                synchronized (server) {
                    timeoutOccurred.set(false);
                    electionScheduler.stopTimeouts();
                }
                pauseMillis(TIMEOUT_MILLIS);
                assertThat(timeoutOccurred.get()).isFalse();
            });
        }

        @Test
        void willThrowWhenTimeoutsAreNotStarted() {
            assertThatThrownBy(() -> electionScheduler.stopTimeouts()).isInstanceOf(IllegalStateException.class);
        }
    }
}