package au.id.tindall.distalg.raft.client.responses;

import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.NOT_LEADER;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class PendingClientRequestResponseTest {

    private PendingClientRequestResponse<Object> response;

    @BeforeEach
    void setUp() {
        response = new PendingClientRequestResponse<>();
    }

    @Test
    void shouldReturnFuture() {
        CompletableFuture<ClientRequestResponse<Object>> responseFuture = response.getResponseFuture();
        assertThat(responseFuture)
                .isNotNull()
                .isNotCompleted();
    }

    @Test
    void shouldFail() throws ExecutionException, InterruptedException {
        response.fail();
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new ClientRequestResponse<>(NOT_LEADER, null, null));
    }
}
