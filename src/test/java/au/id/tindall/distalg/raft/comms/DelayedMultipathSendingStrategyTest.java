package au.id.tindall.distalg.raft.comms;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.server.RequestVoteRequest;
import au.id.tindall.distalg.raft.util.ThreadUtil;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DelayedMultipathSendingStrategyTest {

    @Test
    void returnsMessages() {
        final DelayedMultipathSendingStrategy delayedMultipathSendingStrategy = new DelayedMultipathSendingStrategy(0.0f, 500, 1000);
        delayedMultipathSendingStrategy.onStart(123L);
        final RequestVoteRequest<Long> message = new RequestVoteRequest<>(Term.ZERO, 456L, 789, Optional.empty());
        for (int i = 0; i < 500; i++) {
            delayedMultipathSendingStrategy.send(123L, message);
        }
        ThreadUtil.pauseMillis(1);
        for (int i = 0; i < 500; i++) {
            assertNotNull(delayedMultipathSendingStrategy.poll(123L));
        }
    }
}