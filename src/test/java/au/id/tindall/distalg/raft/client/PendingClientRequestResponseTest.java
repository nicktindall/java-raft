package au.id.tindall.distalg.raft.client;

import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.NOT_LEADER;
import static au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PendingClientRequestResponseTest {

    private static final byte[] COMMAND_BYTES = "command".getBytes();
    private static final byte[] RESULT_BYTES = "result".getBytes();

    private PendingClientRequestResponse<Serializable> response;

    @Mock
    private StateMachine stateMachine;

    @BeforeEach
    void setUp() {
        response = new PendingClientRequestResponse<>(stateMachine);
    }

    @Test
    void shouldReturnFuture() {
        CompletableFuture<ClientRequestResponse<Serializable>> responseFuture = response.getResponseFuture();
        assertThat(responseFuture).isNotNull();
        assertThat(responseFuture).isNotCompleted();
    }

    @Test
    void shouldCompleteSuccessfully() throws ExecutionException, InterruptedException {
        when(stateMachine.apply(COMMAND_BYTES)).thenReturn(RESULT_BYTES);
        response.completeSuccessfully(new StateMachineCommandEntry(new Term(0), COMMAND_BYTES));
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new ClientRequestResponse<>(OK, RESULT_BYTES, null));
    }

    @Test
    void shouldFail() throws ExecutionException, InterruptedException {
        response.fail();
        assertThat(response.getResponseFuture().get()).usingRecursiveComparison()
                .isEqualTo(new ClientRequestResponse<>(NOT_LEADER, null, null));
    }
}
