package au.id.tindall.distalg.raft.serverstates.clustermembership;

import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.rpc.clustermembership.AddServerResponse;
import au.id.tindall.distalg.raft.rpc.clustermembership.RemoveServerResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterMembershipChangeManagerTest {

    private static final int NEW_SERVER_ID = 1234;
    private static final int OLD_SERVER_ID = 5678;

    @Mock
    private ClusterMembershipChangeFactory<Integer> changeFactory;
    @Mock
    private AddServer<Integer> addServer;
    @Mock
    private CompletableFuture<AddServerResponse> addServerResponseFuture;
    @Mock
    private RemoveServer<Integer> removeServer;
    @Mock
    private CompletableFuture<RemoveServerResponse> removeServerResponseFuture;
    @Mock
    private LogEntry logEntry;

    private ClusterMembershipChangeManager<Integer> changeManager;

    @BeforeEach
    void setUp() {
        lenient().when(changeFactory.createAddServer(NEW_SERVER_ID)).thenReturn(addServer);
        lenient().when(addServer.getResponseFuture()).thenReturn(addServerResponseFuture);
        lenient().when(changeFactory.createRemoveServer(OLD_SERVER_ID)).thenReturn(removeServer);
        lenient().when(removeServer.getResponseFuture()).thenReturn(removeServerResponseFuture);
        changeManager = new ClusterMembershipChangeManager<>(changeFactory);
    }

    @Nested
    class AddServerMethod {

        @Test
        void willReturnResponseFuture() {
            assertThat(changeManager.addServer(NEW_SERVER_ID)).isSameAs(addServerResponseFuture);
        }

        @Nested
        class WhenNoChangeIsInProgress {

            @Test
            void willImmediatelyStartAddServerChange() {
                changeManager.addServer(NEW_SERVER_ID);
                verify(addServer).start();
            }
        }

        @Nested
        class WhenAnotherChangeIsInProgress {

            @BeforeEach
            void setUp() {
                changeManager.removeServer(OLD_SERVER_ID);
            }

            @Test
            void willQueueAndNotStartIfCurrentChangeIsNotFinished() {
                changeManager.addServer(NEW_SERVER_ID);
                verify(addServer, never()).start();
            }

            @Test
            void willStartChangeImmediatelyIfCurrentChangeIsFinished() {
                when(removeServer.isFinished()).thenReturn(true);
                changeManager.addServer(NEW_SERVER_ID);
                verify(addServer).start();
            }
        }
    }

    @Nested
    class RemoveServerMethod {

        @Test
        void willReturnResponseFuture() {
            assertThat(changeManager.removeServer(OLD_SERVER_ID)).isSameAs(removeServerResponseFuture);
        }

        @Nested
        class WhenNoChangeIsInProgress {

            @Test
            void willImmediatelyStartRemoveServerChange() {
                changeManager.removeServer(OLD_SERVER_ID);
                verify(removeServer).start();
            }
        }

        @Nested
        class WhenAnotherChangeIsInProgress {

            @BeforeEach
            void setUp() {
                changeManager.addServer(NEW_SERVER_ID);
            }

            @Test
            void willQueueAndNotStartIfCurrentChangeIsNotFinished() {
                changeManager.removeServer(OLD_SERVER_ID);
                verify(removeServer, never()).start();
            }

            @Test
            void willStartChangeImmediatelyIfCurrentChangeIsFinished() {
                when(addServer.isFinished()).thenReturn(true);
                changeManager.removeServer(OLD_SERVER_ID);
                verify(removeServer).start();
            }
        }
    }

    @Nested
    class LogSuccessResponse {

        @Test
        void willDoNothingWhenThereIsNoCurrentChange() {
            changeManager.logSuccessResponse(123, 456);
        }

        @Test
        void willDoNothingWhenTheCurrentChangeIsFinished() {
            changeManager.addServer(NEW_SERVER_ID);
            when(addServer.isFinished()).thenReturn(true);
            changeManager.logSuccessResponse(123, 456);
            verify(addServer, never()).logSuccessResponse(123, 456);
        }

        @Test
        void willDelegateToUnfinishedCurrentChange() {
            changeManager.addServer(NEW_SERVER_ID);
            changeManager.logSuccessResponse(123, 456);
            verify(addServer).logSuccessResponse(123, 456);
        }
    }

    @Nested
    class LogFailureResponse {

        @Test
        void willDoNothingWhenThereIsNoCurrentChange() {
            changeManager.logFailureResponse(123);
        }

        @Test
        void willDoNothingWhenTheCurrentChangeIsFinished() {
            changeManager.addServer(NEW_SERVER_ID);
            when(addServer.isFinished()).thenReturn(true);
            changeManager.logFailureResponse(123);
            verify(addServer, never()).logFailureResponse(123);
        }

        @Test
        void willDelegateToUnfinishedCurrentChange() {
            changeManager.addServer(NEW_SERVER_ID);
            changeManager.logFailureResponse(123);
            verify(addServer).logFailureResponse(123);
        }
    }

    @Nested
    class EntryCommitted {

        @Test
        void willDoNothingIfThereIsNoCurrentChange() {
            changeManager.entryCommitted(123, logEntry);
        }

        @Test
        void willDelegateToCurrentChange() {
            changeManager.addServer(NEW_SERVER_ID);
            changeManager.entryCommitted(123, logEntry);
            verify(addServer).entryCommitted(123);
        }

        @Test
        void willStartNextChangeIfCurrentChangeCompletes() {
            changeManager.addServer(NEW_SERVER_ID);
            changeManager.removeServer(OLD_SERVER_ID);
            when(addServer.isFinished()).thenReturn(true);
            changeManager.entryCommitted(123, logEntry);
            verify(addServer).entryCommitted(123);
            verify(removeServer).start();
        }
    }
}