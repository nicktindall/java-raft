package au.id.tindall.distalg.raft.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static au.id.tindall.distalg.raft.util.ThreadUtil.pauseMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class HeartbeatReplicationSchedulerTest {

    private static final long SERVER_ID = 1234;

    @Mock
    private Supplier<Boolean> sendAppendEntriesRequest;

    private HeartbeatReplicationScheduler scheduler;

    @BeforeEach
    void setUp() {
        lenient().when(sendAppendEntriesRequest.get()).thenReturn(true);
    }

    @Nested
    class WhenRunning {

        @Test
        void willSendRegularHeartbeats() {
            scheduler = new HeartbeatReplicationScheduler<>(SERVER_ID, 500, Executors.newSingleThreadExecutor());
            scheduler.setSendAppendEntriesRequest(sendAppendEntriesRequest);
            scheduler.start();
            await().atMost(2, SECONDS).untilAsserted(() ->
                    verify(sendAppendEntriesRequest, atLeast(2)).get()
            );
        }

        @Test
        void willSendAppendEntriesRequestsOnDemand() {
            scheduler = new HeartbeatReplicationScheduler<>(SERVER_ID, Long.MAX_VALUE, Executors.newSingleThreadExecutor());
            scheduler.setSendAppendEntriesRequest(sendAppendEntriesRequest);
            scheduler.start();
            scheduler.replicate();
            await().atMost(1, SECONDS).untilAsserted(() ->
                    verify(sendAppendEntriesRequest).get()
            );
            Mockito.clearInvocations(sendAppendEntriesRequest);
            scheduler.replicate();
            await().atMost(1, SECONDS).untilAsserted(() ->
                    verify(sendAppendEntriesRequest).get()
            );
        }

        @AfterEach
        void tearDown() {
            scheduler.stop();
        }
    }

    @Nested
    class WhenNotRunning {

        @Test
        void willNotSendRegularHeartbeats() {
            scheduler = new HeartbeatReplicationScheduler<>(SERVER_ID, 100, Executors.newSingleThreadExecutor());
            scheduler.setSendAppendEntriesRequest(sendAppendEntriesRequest);
            pauseMillis(500L);
            verifyNoInteractions(sendAppendEntriesRequest);
        }

        @Test
        void willNotSendAppendEntriesRequestsOnDemand() {
            scheduler = new HeartbeatReplicationScheduler<>(SERVER_ID, Long.MAX_VALUE, Executors.newSingleThreadExecutor());
            scheduler.setSendAppendEntriesRequest(sendAppendEntriesRequest);
            scheduler.replicate();
            pauseMillis(1000L);
            verifyNoInteractions(sendAppendEntriesRequest);
        }
    }
}