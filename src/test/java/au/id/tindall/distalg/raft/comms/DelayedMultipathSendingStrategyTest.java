package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.util.ThreadUtil;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DelayedMultipathSendingStrategyTest {

    private static final RequestVoteRequest<Long> MESSAGE = new RequestVoteRequest<>(Term.ZERO, 456L, 789, Optional.empty());

    @Test
    void returnsMessages() {
        final DelayedMultipathSendingStrategy delayedMultipathSendingStrategy = new DelayedMultipathSendingStrategy(0.0f, 500, 1000);
        delayedMultipathSendingStrategy.onStart(123L);
        for (int i = 0; i < 500; i++) {
            delayedMultipathSendingStrategy.send(123L, MESSAGE);
        }
        ThreadUtil.pauseMillis(1);
        for (int i = 0; i < 500; i++) {
            assertNotNull(delayedMultipathSendingStrategy.poll(123L));
        }
    }

    @Test
    void supportsFixedLatency() {
        final DelayedMultipathSendingStrategy delayedMultipathSendingStrategy = new DelayedMultipathSendingStrategy(0.0f, 0, 0);
        delayedMultipathSendingStrategy.onStart(123L);
        delayedMultipathSendingStrategy.send(123L, MESSAGE);
        assertNotNull(delayedMultipathSendingStrategy.poll(123L));
    }
}