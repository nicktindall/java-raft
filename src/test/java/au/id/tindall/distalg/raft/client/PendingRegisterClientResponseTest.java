package au.id.tindall.distalg.raft.client;

import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.NOT_LEADER;
import static au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.ClientRegistrationEntry;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PendingRegisterClientResponseTest {

    private static final int CLIENT_ID = 1234;
    private ClientRegistrationEntry clientRegistrationEntry;
    private PendingRegisterClientResponse<Serializable> response;

    @BeforeEach
    void setUp() {
        response = new PendingRegisterClientResponse<>();
        clientRegistrationEntry = new ClientRegistrationEntry(new Term(0), CLIENT_ID);
    }

    @Test
    void shouldReturnFuture() {
        CompletableFuture<RegisterClientResponse<Serializable>> responseFuture = response.getResponseFuture();
        assertThat(responseFuture).isNotCompleted();
    }

    @Test
    void shouldCompleteSuccessfully() throws ExecutionException, InterruptedException {
        response.completeSuccessfully(clientRegistrationEntry);
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new RegisterClientResponse<>(OK, CLIENT_ID, null));
    }

    @Test
    public void shouldFail() throws ExecutionException, InterruptedException {
        response.fail();
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new RegisterClientResponse<>(NOT_LEADER, null, null));
    }
}
