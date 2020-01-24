package au.id.tindall.distalg.raft.client.responses;

import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.NOT_LEADER;
import static org.assertj.core.api.Assertions.assertThat;

public class PendingRegisterClientResponseTest {

    private PendingRegisterClientResponse<Serializable> response;

    @BeforeEach
    void setUp() {
        response = new PendingRegisterClientResponse<>();
    }

    @Test
    void shouldReturnFuture() {
        CompletableFuture<RegisterClientResponse<Serializable>> responseFuture = response.getResponseFuture();
        assertThat(responseFuture).isNotCompleted();
    }

    @Test
    public void shouldFail() throws ExecutionException, InterruptedException {
        response.fail();
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new RegisterClientResponse<>(NOT_LEADER, null, null));
    }
}
