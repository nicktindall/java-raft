package au.id.tindall.distalg.raft.clusterclient;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.client.ClientResponseMessage;
import au.id.tindall.distalg.raft.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AbstractClusterClientTest {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Set<Integer> ALL_NODES = Set.of(1, 2, 3);

    @Mock
    private ClusterClient<Integer> clusterClient;

    @BeforeEach
    void setUp() {
        lenient().when(clusterClient.getClusterNodeIds()).thenReturn(ALL_NODES);
    }

    @Test
    void successfulMessage() {
        when(clusterClient.send(anyInt(), any())).thenReturn(CompletableFuture.completedFuture(new TestResponse(true)));
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, 5, 500) {
        }) {
            CompletableFuture<TestResponse> result = abstractClusterClient.sendClientRequest(new TestMessage(), 1_000);
            await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
            assertThat(result).isCompletedWithValue(new TestResponse(true));
        }
        verify(clusterClient).send(anyInt(), any());
    }

    @Test
    void unsuccessfulMessage() {
        when(clusterClient.send(anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(new TestResponse(false)))
                .thenReturn(CompletableFuture.completedFuture(new TestResponse(false)))
                .thenReturn(CompletableFuture.completedFuture(new TestResponse(true)));
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, 5, 500) {
        }) {
            CompletableFuture<TestResponse> result = abstractClusterClient.sendClientRequest(new TestMessage(), 200);
            await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
            assertThat(result).isCompletedWithValue(new TestResponse(true));
        }
        verify(clusterClient).send(eq(1), any());
        verify(clusterClient).send(eq(2), any());
        verify(clusterClient).send(eq(3), any());
    }

    @Test
    void cannotFindLeader() {
        when(clusterClient.send(anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(new TestResponse(false)));
        final int maxRetries = 5;
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, maxRetries, 500) {
        }) {
            CompletableFuture<TestResponse> result = abstractClusterClient.sendClientRequest(new TestMessage(), 200);
            await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
            assertThat(result).isCompletedExceptionally();
            assertThat(result.exceptionNow()).isInstanceOf(AbstractClusterClient.SendFailedException.class);
            assertThat(result.exceptionNow().getSuppressed()).hasSize(maxRetries);
            for (Throwable throwable : result.exceptionNow().getSuppressed()) {
                assertThat(throwable).isInstanceOf(AbstractClusterClient.NotLeaderException.class);
            }
        }
    }

    @Test
    void gotErrorResponse() {
        when(clusterClient.send(anyInt(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("This is broken")));
        final int maxRetries = 5;
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, maxRetries, 500) {
        }) {
            CompletableFuture<TestResponse> result = abstractClusterClient.sendClientRequest(new TestMessage(), 200);
            await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
            assertThat(result).isCompletedExceptionally();
            assertThat(result.exceptionNow()).isInstanceOf(AbstractClusterClient.SendFailedException.class);
            assertThat(result.exceptionNow().getSuppressed()).hasSize(maxRetries);
            for (Throwable throwable : result.exceptionNow().getSuppressed()) {
                assertThat(throwable).isInstanceOf(IllegalStateException.class);
            }
        }
    }

    @Test
    void requestTimeouts() {
        when(clusterClient.send(anyInt(), any()))
                .thenReturn(new CompletableFuture<>());
        final int numRetries = 5;
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, numRetries, 500) {
        }) {
            CompletableFuture<TestResponse> result = abstractClusterClient.sendClientRequest(new TestMessage(), 10);
            await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
            assertThat(result).isCompletedExceptionally();
            Throwable thrownException = result.exceptionNow();
            assertThat(thrownException).isInstanceOf(AbstractClusterClient.SendFailedException.class);
            assertThat(thrownException.getSuppressed()).hasSize(numRetries);
            for (Throwable throwable : result.exceptionNow().getSuppressed()) {
                assertThat(throwable).isInstanceOf(TimeoutException.class);
            }
        }
    }

    @Test
    void closeWillCloseAfterOutstandingRequestsAreClear() {
        when(clusterClient.send(anyInt(), any())).thenAnswer(iom -> {
            CompletableFuture<ClientResponseMessage> future = new CompletableFuture<>();
            new Thread(() -> {
                ThreadUtil.pauseMillis(40);
                LOGGER.info("Completed future");
                future.complete(new TestResponse(false));
            }).start();
            return future;
        });
        final int numRetries = 5;
        final CompletableFuture<TestResponse> result;
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, numRetries, 500) {
        }) {
            result = abstractClusterClient.sendClientRequest(new TestMessage(), 10);
        }
        await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
    }

    @Test
    void sendingOnClosedClientThrowsException() {
        AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, 5, 500) {
        };
        abstractClusterClient.close();
        assertThatThrownBy(() -> abstractClusterClient.sendClientRequest(new TestMessage(), 10))
                .isInstanceOf(AbstractClusterClient.ClientClosedException.class);
        verifyNoInteractions(clusterClient);
    }

    @Test
    void willUseLeaderHintWhenPresent() {
        final AtomicInteger firstAttempt = new AtomicInteger();
        final AtomicInteger suppliedHint = new AtomicInteger();
        when(clusterClient.send(anyInt(), any()))
                .thenAnswer(iom -> {
                    firstAttempt.set(iom.getArgument(0));
                    suppliedHint.set(randomNodeOtherThan(firstAttempt.get()));
                    return CompletableFuture.completedFuture(new TestResponse(false, suppliedHint.get()));
                })
                .thenReturn(CompletableFuture.completedFuture(new TestResponse(true)));
        final int numRetries = 5;
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, numRetries, 500) {
        }) {
            CompletableFuture<TestResponse> result = abstractClusterClient.sendClientRequest(new TestMessage(), 10);
            await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
            assertThat(result).isCompleted();
            InOrder sequence = inOrder(clusterClient);
            sequence.verify(clusterClient).send(eq(firstAttempt.get()), any());
            sequence.verify(clusterClient).send(eq(suppliedHint.get()), any());
        }
    }

    @Test
    void willIgnoreLeaderHintIfSameServer() {
        final AtomicInteger firstAttempt = new AtomicInteger();
        when(clusterClient.send(anyInt(), any()))
                .thenAnswer(iom -> {
                    firstAttempt.set(iom.getArgument(0));
                    return CompletableFuture.completedFuture(new TestResponse(false, firstAttempt.get()));
                })
                .thenReturn(CompletableFuture.completedFuture(new TestResponse(true)));
        final int numRetries = 5;
        try (AbstractClusterClient<Integer> abstractClusterClient = new AbstractClusterClient<>(clusterClient, 10, 2, numRetries, 500) {
        }) {
            CompletableFuture<TestResponse> result = abstractClusterClient.sendClientRequest(new TestMessage(), 10);
            await().atMost(5, TimeUnit.SECONDS).until(result::isDone);
            assertThat(result).isCompleted();
            InOrder sequence = inOrder(clusterClient);
            sequence.verify(clusterClient).send(eq(firstAttempt.get()), any());
            sequence.verify(clusterClient).send(not(eq(firstAttempt.get())), any());
        }
    }

    private int randomNodeOtherThan(int nodeId) {
        List<Integer> otherNodes = ALL_NODES.stream()
                .filter(id -> id != null && id != nodeId)
                .toList();
        return otherNodes.get(ThreadLocalRandom.current().nextInt(otherNodes.size()));
    }

    private record TestResponse(boolean fromLeader, Integer leaderHint) implements ClientResponseMessage<Integer> {

        public TestResponse(boolean fromLeader) {
            this(fromLeader, null);
        }

        @Override
        public boolean isFromLeader() {
            return fromLeader;
        }

        @Override
        public Integer getLeaderHint() {
            return leaderHint;
        }
    }

    private static class TestMessage implements ClientRequestMessage<Integer, TestResponse> {
    }
}