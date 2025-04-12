package au.id.tindall.distalg.raft.processors;

import au.id.tindall.distalg.raft.Server;
import au.id.tindall.distalg.raft.comms.Cluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InboxProcessorTest {

    @Mock
    private Server<Integer> server;
    @Mock
    private Cluster<Integer> cluster;
    @Mock
    private Runnable onStart;
    @Mock
    private Runnable onStop;

    private InboxProcessor<Integer> inboxProcessor;

    @BeforeEach
    void setUp() {
        inboxProcessor = new InboxProcessor<>(server, cluster, onStart, onStop);
    }

    @Test
    void beforeFirstWillCallOnStart() {
        inboxProcessor.beforeFirst();
        verify(onStart).run();
    }

    @Test
    void afterLastWillCallOnStop() {
        inboxProcessor.afterLast();
        verify(onStop).run();
    }

    @Test
    void processWillProcessAllMessagesThenReturnBusy() {
        when(cluster.processNextMessage(server)).thenReturn(true, true, true, false);
        assertEquals(Processor.ProcessResult.BUSY, inboxProcessor.process());
        verify(cluster, times(4)).processNextMessage(server);
    }

    @Test
    void processWillReturnIdleWhenThereAreNoMessages() {
        when(cluster.processNextMessage(server)).thenReturn(false);
        assertEquals(Processor.ProcessResult.IDLE, inboxProcessor.process());
        verify(cluster).processNextMessage(server);
    }
}