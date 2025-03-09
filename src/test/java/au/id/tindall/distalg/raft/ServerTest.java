package au.id.tindall.distalg.raft;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.comms.Inbox;
import au.id.tindall.distalg.raft.elections.ElectionScheduler;
import au.id.tindall.distalg.raft.elections.ElectionTimeoutProcessor;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.processors.InboxProcessor;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.rpc.server.TimeoutNowMessage;
import au.id.tindall.distalg.raft.serverstates.Follower;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import au.id.tindall.distalg.raft.state.PersistentState;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerTest {

    private static final long SERVER_ID = 100L;
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_0 = new Term(0);

    @Mock
    private ServerStateFactory<Long> serverStateFactory;
    @Mock
    private StateMachine stateMachine;
    @Mock
    private Follower<Long> serverState;
    @Mock
    private PersistentState<Long> persistentState;
    @Mock
    private Cluster<Long> cluster;
    @Mock
    private ElectionScheduler electionScheduler;
    private TestProcessorManager processorManager;
    @Mock
    private Inbox<Long> inbox;

    private Server<Long> server;

    @BeforeEach
    void setUp() {
        processorManager = new TestProcessorManager();
        server = new ServerImpl<>(persistentState, serverStateFactory, stateMachine, cluster, electionScheduler, processorManager, inbox);
    }

    @Nested
    class Constructor {

        @Test
        void willNotInitializeState() {
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
            assertThat(server.getState()).contains(FOLLOWER);
        }

        @Test
        void willEnterInitialState() {
            verify(serverState).enterState();
        }

        @Test
        void willThrowIfAlreadyStarted() {
            assertThatThrownBy(() -> server.start())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Can't start, server is already started!");
        }
    }

    @Nested
    class Stop {

        @BeforeEach
        void setUp() {
            when(serverStateFactory.createInitialState()).thenReturn(serverState);
            server.start();
            server.stop();
        }

        @Test
        void willStopInboxProcessor() {
            verify(processorManager.getProcessorController(InboxProcessor.class)).stopAndWait();
        }

        @Test
        void willStopElectionTimeoutProcessor() {
            verify(processorManager.getProcessorController(ElectionTimeoutProcessor.class)).stopAndWait();
        }
    }

    @Nested
    class TimeoutNowIfDue {

        @BeforeEach
        void setUp() {
            lenient().when(persistentState.getCurrentTerm()).thenReturn(TERM_0);
            lenient().when(serverStateFactory.createInitialState()).thenReturn(serverState);
            lenient().when(persistentState.getId()).thenReturn(SERVER_ID);
            server.start();
        }

        @Test
        @SuppressWarnings({"ConstantConditions", "unchecked"})
        void willDispatchInitiateElectionMessageWithCurrentTerm_WhenTimeoutIsDue() {
            when(electionScheduler.shouldTimeout()).thenReturn(true);
            when(serverState.handle(any(RpcMessage.class))).thenReturn(complete(serverState));
            server.timeoutNowIfDue();

            verify(serverState).handle(refEq(new TimeoutNowMessage<>(TERM_0, SERVER_ID)));
        }

        @Test
        @SuppressWarnings({"ConstantConditions", "unchecked"})
        void willDoNothing_WhenTimeoutIsNotDue() {
            when(electionScheduler.shouldTimeout()).thenReturn(false);
            server.timeoutNowIfDue();

            verify(serverState, never()).handle(any(TimeoutNowMessage.class));
        }
    }

    @Nested
    class ClientRequests {

        @Nested
        class WhenServerNotStarted {

            @Test
            void willThrowWhenServerIsNotStarted() {
                var clientRequest = new ClientRequestMessage<>() {
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
                var clientRequest = new ClientRequestMessage<>() {
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
            void willContinueHandling_WhenHandlingIsIncomplete() {
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

    @Nested
    class Close {

        @Nested
        class WhenServerIsRunning {

            @BeforeEach
            void setUp() {
                when(serverStateFactory.createInitialState()).thenReturn(serverState);
                server.start();
                reset(serverState);
            }

            @Test
            void willStopServerThenClose() {
                server.close();
                verify(processorManager.getProcessorController(InboxProcessor.class)).stopAndWait();
                verify(processorManager.getProcessorController(ElectionTimeoutProcessor.class)).stopAndWait();
                verify(serverStateFactory).close();
                verify(persistentState).close();
                assertThat(processorManager.isClosed()).isTrue();
            }
        }

        @Test
        void willCloseServerStateFactory() {
            server.close();
            verify(serverStateFactory).close();
        }
    }
}