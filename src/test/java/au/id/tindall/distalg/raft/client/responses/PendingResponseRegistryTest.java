package au.id.tindall.distalg.raft.client.responses;

import au.id.tindall.distalg.raft.client.sessions.ClientSessionCreatedHandler;
import au.id.tindall.distalg.raft.client.sessions.ClientSessionStore;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestResponse;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestStatus;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.client.RegisterClientStatus;
import au.id.tindall.distalg.raft.statemachine.CommandAppliedEventHandler;
import au.id.tindall.distalg.raft.statemachine.CommandExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PendingResponseRegistryTest {

    private static final int LOG_INDEX = 456;
    private static final int OTHER_LOG_INDEX = 789;
    private PendingResponseRegistry pendingResponseRegistry;

    @Mock
    private PendingResponse<?> pendingResponse;
    @Mock
    private PendingResponse<?> otherPendingResponse;
    @Mock
    private ClientSessionStore clientSessionStore;
    @Mock
    private CommandExecutor commandExecutor;
    private ClientSessionCreatedHandler clientSessionCreatedHandler;
    private CommandAppliedEventHandler commandAppliedEventHandler;

    @BeforeEach
    void setUp() {
        doAnswer(invocation -> {
            this.clientSessionCreatedHandler = invocation.getArgument(0);
            return null;
        }).when(clientSessionStore).addClientSessionCreatedHandler(any(ClientSessionCreatedHandler.class));
        doAnswer(invocation -> {
            this.commandAppliedEventHandler = invocation.getArgument(0);
            return null;
        }).when(commandExecutor).addCommandAppliedEventHandler(any(CommandAppliedEventHandler.class));
        pendingResponseRegistry = new PendingResponseRegistry(clientSessionStore, commandExecutor);
    }

    @Nested
    class ConstructorAndDispose {

        @Test
        void constructorAndDispose_WillAddAndRemoveSameClientSessionHandler() {
            ArgumentCaptor<ClientSessionCreatedHandler> sessionCreatedHandlerCaptor = forClass(ClientSessionCreatedHandler.class);
            verify(clientSessionStore).addClientSessionCreatedHandler(sessionCreatedHandlerCaptor.capture());
            pendingResponseRegistry.dispose();
            verify(clientSessionStore).removeClientSessionCreatedHandler(sessionCreatedHandlerCaptor.getValue());
        }

        @Test
        void constructorAndDispose_WillAddAndRemoveSameCommandAppliedHandler() {
            ArgumentCaptor<CommandAppliedEventHandler> commandAppliedHandlerCaptor = forClass(CommandAppliedEventHandler.class);
            verify(commandExecutor).addCommandAppliedEventHandler(commandAppliedHandlerCaptor.capture());
            pendingResponseRegistry.dispose();
            verify(commandExecutor).removeCommandAppliedEventHandler(commandAppliedHandlerCaptor.getValue());
        }

        @Test
        void dispose_WillFailAllOutstandingResponses() {
            pendingResponseRegistry.registerOutstandingResponse(LOG_INDEX, pendingResponse);
            pendingResponseRegistry.registerOutstandingResponse(OTHER_LOG_INDEX, otherPendingResponse);
            pendingResponseRegistry.dispose();
            verify(pendingResponse).fail();
            verify(otherPendingResponse).fail();
        }
    }

    @Nested
    class HandleCommandApplied {

        private final byte[] RESULT = "result".getBytes();
        private final int CLIENT_ID = 1234;
        private final int SEQUENCE_NUMBER = 0;

        @Mock
        private CompletableFuture<ClientRequestResponse<Integer>> responseFuture;

        @Test
        void willCompleteResponse_WhenOneIsRegisteredAtTheCommittedIndex() {
            when(pendingResponse.getResponseFuture()).thenReturn((CompletableFuture) responseFuture);

            pendingResponseRegistry.registerOutstandingResponse(LOG_INDEX, pendingResponse);
            commandAppliedEventHandler.handleCommandApplied(LOG_INDEX, CLIENT_ID, -1, SEQUENCE_NUMBER, RESULT);

            verify(responseFuture).complete(refEq(new ClientRequestResponse<>(ClientRequestStatus.OK, RESULT, null)));
        }

        @Test
        void willDoNothing_WhenThereIsNoResponseRegistered() {
            commandAppliedEventHandler.handleCommandApplied(LOG_INDEX, CLIENT_ID, -1, SEQUENCE_NUMBER, RESULT);
        }
    }

    @Nested
    class HandleSessionCreated {

        private final int CLIENT_ID = 555;

        @Mock
        private CompletableFuture<RegisterClientResponse<Integer>> responseFuture;

        @Test
        void willCompleteResponse_WhenOneIsRegisteredAtTheCommittedIndex() {
            when(pendingResponse.getResponseFuture()).thenReturn((CompletableFuture) responseFuture);

            pendingResponseRegistry.registerOutstandingResponse(LOG_INDEX, pendingResponse);
            clientSessionCreatedHandler.clientSessionCreated(LOG_INDEX, CLIENT_ID);

            verify(responseFuture).complete(refEq(new RegisterClientResponse<>(RegisterClientStatus.OK, CLIENT_ID, null)));
        }

        @Test
        void willDoNothing_WhenThereIsNoResponseRegistered() {
            clientSessionCreatedHandler.clientSessionCreated(LOG_INDEX, CLIENT_ID);
        }
    }

    @Test
    void registerPendingResponse_WillReturnPendingResponseFuture() {
        CompletableFuture future = new CompletableFuture();
        when(pendingResponse.getResponseFuture()).thenReturn(future);
        assertThat(pendingResponseRegistry.registerOutstandingResponse(LOG_INDEX, pendingResponse)).isSameAs(future);
    }
}