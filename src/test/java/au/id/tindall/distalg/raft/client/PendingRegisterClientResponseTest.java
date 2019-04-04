package au.id.tindall.distalg.raft.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PendingRegisterClientResponseTest {

    private static final int CLIENT_ID = 1234;
    private ClientRegistrationEntry clientRegistrationEntry;

    @BeforeEach
    void setUp() {
        clientRegistrationEntry = new ClientRegistrationEntry(new Term(0), CLIENT_ID);
    }

    @Test
    void shouldReturnFuture() {
        CompletableFuture<RegisterClientResponse<Serializable>> responseFuture = new PendingRegisterClientResponse<>().getResponseFuture();
        assertThat(responseFuture).isInstanceOf(CompletableFuture.class);
        assertThat(responseFuture).isNotCompleted();
    }

    @Test
    void shouldResolveFutureOnSuccess() throws ExecutionException, InterruptedException {
        var response = new PendingRegisterClientResponse<>();
        response.completeSuccessfully(clientRegistrationEntry);
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new RegisterClientResponse<>(RegisterClientStatus.OK, CLIENT_ID, null));
    }

    @Test
    public void shouldResolveFutureOnFailure() throws ExecutionException, InterruptedException {
        var response = new PendingRegisterClientResponse<>();
        response.generateFailedResponse();
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, null));
    }
}
