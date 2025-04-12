package au.id.tindall.distalg.raft.comms.simulated;

import au.id.tindall.distalg.raft.comms.MessageProcessor;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class DelayingReorderingRouterTest {

    @Test
    void delaysMessages() {
        AtomicLong nanoTime = new AtomicLong();
        final int maximumDelayMicros = 1000;
        final int minimumDelayMicros = 500;
        final DelayingReorderingRouter<Long> router = new DelayingReorderingRouter<Long>(nanoTime::get, 0.0f, minimumDelayMicros, maximumDelayMicros, TimeUnit.MICROSECONDS);

        try (final QueueingCluster<Long> sourceCluster = new QueueingCluster<>(123L);
             final QueueingCluster<Long> destinationCluster = new QueueingCluster<>(456L)) {
            final Map<Long, QueueingCluster<Long>> clusters = Map.of(123L, sourceCluster, 456L, destinationCluster);
            for (int i = 0; i < 500; i++) {
                sourceCluster.sendTimeoutNowRequest(Term.ZERO, 456L);
            }
            router.route(clusters);
            assertReceivedMessageCount(0, destinationCluster);

            nanoTime.set(TimeUnit.MICROSECONDS.toNanos(minimumDelayMicros - 1));
            router.route(clusters);
            assertReceivedMessageCount(0, destinationCluster);

            nanoTime.set(TimeUnit.MICROSECONDS.toNanos(maximumDelayMicros + 1));
            router.route(clusters);
            assertReceivedMessageCount(500, destinationCluster);
        }
    }

    private void assertReceivedMessageCount(int expectedCount, QueueingCluster<Long> cluster) {
        CountingMessageProcessor messageProcessor = new CountingMessageProcessor();
        while (cluster.processNextMessage(messageProcessor)) {
            // repeat
        }
        assertThat(messageProcessor.getCount()).isEqualTo(expectedCount);
    }

    @Test
    void supportsFixedLatency() {
        final DelayingReorderingRouter<Long> router = new DelayingReorderingRouter<>(0.0f, 0, 0, TimeUnit.MICROSECONDS);

        try (final QueueingCluster<Long> sourceCluster = new QueueingCluster<>(123L);
             final QueueingCluster<Long> destinationCluster = new QueueingCluster<>(456L)) {
            final Map<Long, QueueingCluster<Long>> clusters = Map.of(123L, sourceCluster, 456L, destinationCluster);
            sourceCluster.sendTimeoutNowRequest(Term.ZERO, 456L);
            router.route(clusters);
            assertReceivedMessageCount(1, destinationCluster);
        }
    }

    private static final class CountingMessageProcessor implements MessageProcessor<Long> {

        private int count = 0;

        @Override
        public <R extends ClientResponseMessage<Long>> CompletableFuture<R> handle(ClientRequestMessage<Long, R> clientRequestMessage) {
            count++;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void handle(RpcMessage<Long> message) {
            count++;
        }

        public int getCount() {
            return count;
        }
    }
}