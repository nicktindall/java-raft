package au.id.tindall.distalg.raft.driver;

import au.id.tindall.distalg.raft.serverstates.Leader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HeartbeatSchedulerTest {

    private static final long DELAY_MILLIS = 12345L;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;
    @Mock
    private Leader<Long> leader;
    @Mock
    private ScheduledFuture heartbeatFuture;

    private HeartbeatScheduler<Long> heartbeatScheduler;

    @BeforeEach
    void setUp() {
        heartbeatScheduler = new HeartbeatScheduler<>(DELAY_MILLIS, scheduledExecutorService);
    }

    @Nested
    class ScheduleHeartbeats {

        @BeforeEach
        @SuppressWarnings("unchecked")
        void setUp() {
            when(scheduledExecutorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenReturn(heartbeatFuture);
        }

        @Test
        void willScheduleRegularHeartbeats() {
            heartbeatScheduler.scheduleHeartbeats(leader);
            verify(scheduledExecutorService).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(DELAY_MILLIS), eq(MILLISECONDS));
        }

        @Test
        void willThrowIfHeartbeatsAlreadyScheduled() {
            heartbeatScheduler.scheduleHeartbeats(leader);
            assertThatThrownBy(() -> heartbeatScheduler.scheduleHeartbeats(leader)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class CancelHeartbeats {

        @Test
        @SuppressWarnings("unchecked")
        void willCancelScheduledHeartbeat() {
            when(scheduledExecutorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenReturn(heartbeatFuture);
            heartbeatScheduler.scheduleHeartbeats(leader);
            heartbeatScheduler.cancelHeartbeats();
            verify(heartbeatFuture).cancel(false);
        }

        @Test
        void willThrowIfHeartbeatsNotScheduled() {
            assertThatThrownBy(() -> heartbeatScheduler.cancelHeartbeats()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Heartbeat {

        private Runnable heartbeat;

        @BeforeEach
        @SuppressWarnings("unchecked")
        void setUp() {
            when(scheduledExecutorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenReturn(heartbeatFuture);
            heartbeatScheduler.scheduleHeartbeats(leader);
            ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
            verify(scheduledExecutorService).scheduleAtFixedRate(captor.capture(), eq(0L), eq(DELAY_MILLIS), eq(MILLISECONDS));
            heartbeat = captor.getValue();
        }

        @Test
        void willCallSendHeartbeatMessageOnLeader() {
            heartbeat.run();
            verify(leader).sendHeartbeatMessage();
        }
    }
}