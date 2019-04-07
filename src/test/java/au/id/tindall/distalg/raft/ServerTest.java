package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.FOLLOWER;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ServerTest {

    private static final long SERVER_ID = 100L;
    private static final Term RESTORED_TERM = new Term(111);
    private static final long RESTORED_VOTED_FOR = 999L;
    private static final Log RESTORED_LOG = new Log();

    @Mock
    private Cluster<Long> cluster;

    @Nested
    class NewServerConstructor {

        @Test
        public void willSetId() {
            var server = new Server<>(SERVER_ID, cluster);
            assertThat(server.getId()).isEqualTo(SERVER_ID);
        }

        @Test
        public void willInitializeCurrentTermToZero() {
            var server = new Server<>(SERVER_ID, cluster);
            assertThat(server.getCurrentTerm()).isEqualTo(new Term(0));
        }

        @Test
        public void willInitializeLogToEmpty() {
            var server = new Server<>(SERVER_ID, cluster);
            assertThat(server.getLog().getEntries()).isEmpty();
        }

        @Test
        public void willInitializeVotedForToNull() {
            var server = new Server<>(SERVER_ID, cluster);
            assertThat(server.getVotedFor()).isEmpty();
        }

        @Test
        public void willInitializeStateToFollower() {
            var server = new Server<>(SERVER_ID, cluster);
            assertThat(server.getState()).isEqualTo(FOLLOWER);
        }
    }

    @Nested
    class ResetServerConstructor {

        @Test
        public void willSetId() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            assertThat(server.getId()).isEqualTo(SERVER_ID);
        }

        @Test
        public void willRestoreCurrentTerm() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            assertThat(server.getCurrentTerm()).isEqualTo(RESTORED_TERM);
        }

        @Test
        public void willRestoreLog() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            assertThat(server.getLog()).isSameAs(RESTORED_LOG);
        }

        @Test
        public void willRestoreVotedFor() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            assertThat(server.getVotedFor()).contains(RESTORED_VOTED_FOR);
        }

        @Test
        public void willInitializeStateToFollower() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            assertThat(server.getState()).isEqualTo(FOLLOWER);
        }
    }

    @Nested
    class ElectionTimeout {

        @Test
        public void willSetStateToCandidate() {
            var server = new Server<>(SERVER_ID, cluster);
            server.electionTimeout();
            assertThat(server.getState()).isEqualTo(CANDIDATE);
        }

        @Test
        public void willIncrementTerm() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            server.electionTimeout();
            assertThat(server.getCurrentTerm()).isEqualTo(RESTORED_TERM.next());
        }

        @Test
        public void willVoteForSelf() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            when(cluster.isQuorum(singleton(SERVER_ID))).thenReturn(false);
            server.electionTimeout();
            assertThat(server.getVotedFor()).contains(SERVER_ID);
        }

        @Test
        public void willBroadcastRequestVoteRequests() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            server.electionTimeout();
            verify(cluster).sendRequestVoteRequest(RESTORED_TERM.next(), RESTORED_LOG.getLastLogIndex(), RESTORED_LOG.getLastLogTerm());
        }

        @Test
        @SuppressWarnings("unchecked")
        public void willTransitionToLeader_WhenOwnVoteIsQuorum() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
            when(cluster.isQuorum(singleton(SERVER_ID))).thenReturn(true);
            server.electionTimeout();
            verify(cluster, never()).sendAppendEntriesRequest(any(Term.class), anyLong(), anyInt(), any(Optional.class), anyList(), anyInt());
            assertThat(server.getState()).isEqualTo(LEADER);
        }
    }

    @Nested
    class ClientRequests {

        @Mock
        private ServerState<Long> serverState;

        @Test
        @SuppressWarnings("unchecked")
        void willBeHandledByTheCurrentState() {
            var clientRequest = new ClientRequestMessage<>(SERVER_ID) {
            };
            var clientResponse = new CompletableFuture();
            when(serverState.handle(clientRequest)).thenReturn(clientResponse);
            var server = new Server<>(SERVER_ID, serverState);
            assertThat(server.handle(clientRequest)).isSameAs(clientResponse);
        }
    }

    @Nested
    class HandleRpcMessage {

        @Mock
        private ServerState<Long> serverState;
        @Mock
        private ServerState<Long> nextServerState;
        @Mock
        private RpcMessage<Long> rpcMessage;

        @Test
        void willDelegateToCurrentState() {
            when(serverState.handle(rpcMessage)).thenReturn(complete(serverState));

            new Server<>(SERVER_ID, serverState).handle(rpcMessage);

            verify(serverState).handle(rpcMessage);
            verifyNoMoreInteractions(serverState);
        }

        @Test
        void willTransitionToNextStateAndDisposeOfPrevious() {
            when(serverState.handle(rpcMessage)).thenReturn(complete(nextServerState));
            when(nextServerState.handle(rpcMessage)).thenReturn(complete(nextServerState));

            Server<Long> server = new Server<>(SERVER_ID, serverState);

            server.handle(rpcMessage);
            verify(serverState).handle(rpcMessage);
            verify(serverState).dispose();

            server.handle(rpcMessage);
            verify(nextServerState).handle(rpcMessage);

            verifyNoMoreInteractions(serverState, nextServerState);
        }

        @Test
        public void willContinueHandling_WhenHandlingIsIncomplete() {
            when(serverState.handle(rpcMessage)).thenReturn(incomplete(nextServerState));
            when(nextServerState.handle(rpcMessage)).thenReturn(complete(nextServerState));

            Server<Long> server = new Server<>(SERVER_ID, serverState);

            server.handle(rpcMessage);
            verify(serverState).handle(rpcMessage);
            verify(serverState).dispose();
            verify(nextServerState).handle(rpcMessage);

            verifyNoMoreInteractions(serverState, nextServerState);
        }
    }
}