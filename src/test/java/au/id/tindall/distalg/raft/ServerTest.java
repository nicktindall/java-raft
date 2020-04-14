package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.server.InitiateElectionMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.Follower;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.statemachine.StateMachine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ServerTest {

    private static final long SERVER_ID = 100L;
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_0 = new Term(0);

    @Mock
    private ServerStateFactory<Long> serverStateFactory;
    @Mock
    private StateMachine stateMachine;
    @Mock
    private Follower<Long> serverState;

    private Server<Long> server;

    @BeforeEach
    void setUp() {
        server = new Server<>(SERVER_ID, serverStateFactory, stateMachine);
    }

    @Nested
    class Constructor {

        @Test
        public void willSetId() {
            assertThat(server.getId()).isEqualTo(SERVER_ID);
        }

        @Test
        public void willNotInitializeState() {
            verifyNoMoreInteractions(serverStateFactory);
        }
    }

    @Nested
    class Start {

        @BeforeEach
        void setUp() {
            when(serverStateFactory.createInitialState()).thenReturn(serverState);
            server.start();
        }

        @Test
        void willInitializeStateToFollower() {
            when(serverState.getServerStateType()).thenReturn(FOLLOWER);
            verify(serverStateFactory).createInitialState();
            assertThat(server.getState()).isEqualTo(FOLLOWER);
        }

        @Test
        void willEnterInitialState() {
            verify(serverState).enterState();
        }
    }

    @Nested
    class ElectionTimeout {

        @BeforeEach
        void setUp() {
            when(serverState.getCurrentTerm()).thenReturn(TERM_0);
            when(serverStateFactory.createInitialState()).thenReturn(serverState);
            server.start();
        }

        @Test
        @SuppressWarnings({"ConstantConditions", "unchecked"})
        public void willDispatchInitiateElectionMessageWithIncrementedTerm() {
            when(serverState.handle(any(RpcMessage.class))).thenReturn(complete(serverState));
            server.electionTimeout();

            verify(serverState).handle(refEq(new InitiateElectionMessage<>(TERM_1, SERVER_ID)));
        }
    }

    @Nested
    class ClientRequests {

        @Nested
        class WhenServerNotStarted {

            @Test
            void willThrowWhenServerIsNotStarted() {
                var clientRequest = new ClientRequestMessage<>(SERVER_ID) {
                };
                assertThatThrownBy(() -> server.handle(clientRequest))
                        .isInstanceOf(IllegalStateException.class);
            }
        }


        @Nested
        class WhenServerStarted {

            @BeforeEach
            void setUp() {
                when(serverStateFactory.createInitialState()).thenReturn(serverState);
                server.start();
            }

            @Test
            @SuppressWarnings("unchecked")
            void willBeHandledByTheCurrentState() {
                var clientRequest = new ClientRequestMessage<>(SERVER_ID) {
                };
                var clientResponse = new CompletableFuture();
                when(serverState.handle(clientRequest)).thenReturn(clientResponse);
                assertThat(server.handle(clientRequest)).isSameAs(clientResponse);
            }
        }
    }

    @Nested
    class HandleRpcMessage {

        @Mock
        private ServerState<Long> nextServerState;
        @Mock
        private RpcMessage<Long> rpcMessage;

        @Nested
        class WhenServerNotStarted {

            @Test
            void willThrow() {
                assertThatThrownBy(() -> server.handle(rpcMessage))
                        .isInstanceOf(IllegalStateException.class);
            }
        }


        @Nested
        class WhenServerStarted {

            @BeforeEach
            void setUp() {
                when(serverStateFactory.createInitialState()).thenReturn(serverState);
                server.start();
                reset(serverState);
            }

            @Test
            void willDelegateToCurrentState() {
                when(serverState.handle(rpcMessage)).thenReturn(complete(serverState));

                server.handle(rpcMessage);

                verify(serverState).handle(rpcMessage);
                verifyNoMoreInteractions(serverState);
            }

            @Test
            void willLeavePreviousStateThenEnterNextState() {
                when(serverState.handle(rpcMessage)).thenReturn(complete(nextServerState));
                when(nextServerState.handle(rpcMessage)).thenReturn(complete(nextServerState));

                InOrder inOrder = inOrder(serverState, nextServerState);
                server.handle(rpcMessage);
                inOrder.verify(serverState).handle(rpcMessage);
                inOrder.verify(serverState).leaveState();
                inOrder.verify(nextServerState).enterState();

                server.handle(rpcMessage);
                inOrder.verify(nextServerState).handle(rpcMessage);

                inOrder.verifyNoMoreInteractions();
            }

            @Test
            public void willContinueHandling_WhenHandlingIsIncomplete() {
                when(serverState.handle(rpcMessage)).thenReturn(incomplete(nextServerState));
                when(nextServerState.handle(rpcMessage)).thenReturn(complete(nextServerState));

                InOrder inOrder = inOrder(serverState, nextServerState);
                server.handle(rpcMessage);
                inOrder.verify(serverState).handle(rpcMessage);
                inOrder.verify(serverState).leaveState();
                inOrder.verify(nextServerState).enterState();
                inOrder.verify(nextServerState).handle(rpcMessage);

                inOrder.verifyNoMoreInteractions();
            }
        }
    }
}