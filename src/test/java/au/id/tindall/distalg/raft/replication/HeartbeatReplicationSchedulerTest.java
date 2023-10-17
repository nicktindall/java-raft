package au.id.tindall.distalg.raft.replication;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class HeartbeatReplicationSchedulerTest {

    private static final long MAX_DELAY_BETWEEN_MESSAGES_MS = 500;

    private HeartbeatReplicationScheduler scheduler;
    private AtomicLong currentTimeProvider;

    @BeforeEach
    void setUp() {
        currentTimeProvider = new AtomicLong(System.currentTimeMillis());
        scheduler = new HeartbeatReplicationScheduler(MAX_DELAY_BETWEEN_MESSAGES_MS, currentTimeProvider::get);
    }

    @Test
    void startWillThrowWhenAlreadyStarted() {
        scheduler.start();
        assertThatThrownBy(() -> scheduler.start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Attempted to start replication scheduler twice");
    }

    @Test
    void stopWillThrowWhenAlreadyStopped() {
        assertThatThrownBy(() -> scheduler.stop())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Attempted to stop non-running replication scheduler");
    }

    @Nested
    class WhenStarted {

        @BeforeEach
        void setUp() {
            scheduler.start();
        }

        @Nested
        class ReplicationIsDue {

            @Test
            void willReturnFalseWhenAMessageWasSentWithinTimeout() {
                scheduler.replicated();
                currentTimeProvider.addAndGet(MAX_DELAY_BETWEEN_MESSAGES_MS - 1);
                assertThat(scheduler.replicationIsDue()).isFalse();
            }

            @Test
            void willReturnTrueWhenNoMessageWasSentWithinTimeout() {
                scheduler.replicated();
                currentTimeProvider.addAndGet(MAX_DELAY_BETWEEN_MESSAGES_MS + 1);
                assertThat(scheduler.replicationIsDue()).isTrue();
            }
        }

        @Test
        void stopWillStopScheduler() {
            scheduler.replicated();
            scheduler.stop();
            currentTimeProvider.addAndGet(MAX_DELAY_BETWEEN_MESSAGES_MS + 1);
            assertThat(scheduler.replicationIsDue()).isFalse();
        }
    }

    @Nested
    class WhenStopped {

        @Nested
        class ReplicationIsDue {

            @Test
            void willReturnFalseWhenAMessageWasSentWithinTimeout() {
                scheduler.replicated();
                currentTimeProvider.addAndGet(MAX_DELAY_BETWEEN_MESSAGES_MS - 1);
                assertThat(scheduler.replicationIsDue()).isFalse();
            }

            @Test
            void willReturnFalseWhenNoMessageWasSentWithinTimeout() {
                scheduler.replicated();
                currentTimeProvider.addAndGet(MAX_DELAY_BETWEEN_MESSAGES_MS + 1);
                assertThat(scheduler.replicationIsDue()).isFalse();
            }
        }
    }
}