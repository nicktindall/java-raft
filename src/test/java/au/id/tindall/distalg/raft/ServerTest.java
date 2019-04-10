package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.CANDIDATE;
import static au.id.tindall.distalg.raft.serverstates.ServerStateType.LEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.client.ClientRequestMessage;
import au.id.tindall.distalg.raft.rpc.server.RpcMessage;
import au.id.tindall.distalg.raft.serverstates.Candidate;
import au.id.tindall.distalg.raft.serverstates.Follower;
import au.id.tindall.distalg.raft.serverstates.Leader;
import au.id.tindall.distalg.raft.serverstates.ServerState;
import au.id.tindall.distalg.raft.serverstates.ServerStateFactory;
import org.junit.jupiter.api.BeforeEach;
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
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_0 = new Term(0);

    @Mock
    private ServerStateFactory<Long> serverStateFactory;

    @Nested
    class NewServerConstructor {

        @Test
        public void willSetId() {
            var server = new Server<>(SERVER_ID, serverStateFactory);
            assertThat(server.getId()).isEqualTo(SERVER_ID);
        }

        @Test
        public void willInitializeStateToFollowerWithTermZeroAndVotedForNull() {
            new Server<>(SERVER_ID, serverStateFactory);
            verify(serverStateFactory).createFollower(new Term(0), null);
        }
    }

    @Nested
    class ResetServerConstructor {

        @Test
        public void willSetId() {
            var server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, serverStateFactory);

            assertThat(server.getId()).isEqualTo(SERVER_ID);
        }

        @Test
        public void willInitializeStateToFollowerWithSpecifiedTermAndVotedFor() {
            new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, serverStateFactory);

            verify(serverStateFactory).createFollower(RESTORED_TERM, RESTORED_VOTED_FOR);
        }
    }

    @Nested
    class ElectionTimeout {

        @Mock
        private Candidate<Long> candidate;
        @Mock
        private Follower<Long> currentState;
        @Mock
        private Leader<Long> leader;

        @BeforeEach
        void setUp() {
            when(currentState.getCurrentTerm()).thenReturn(TERM_0);
            when(serverStateFactory.createCandidate(any(Term.class))).thenReturn(candidate);
            when(candidate.recordVoteAndClaimLeadershipIfEligible(anyLong())).thenReturn(complete(candidate));
        }

        @Test
        public void willSetStateToCandidate() {
            when(candidate.getServerStateType()).thenReturn(CANDIDATE);

            var server = new Server<>(SERVER_ID, currentState, serverStateFactory);
            server.electionTimeout();

            assertThat(server.getState()).isEqualTo(CANDIDATE);
        }

        @Test
        public void willIncrementTerm() {
            var server = new Server<>(SERVER_ID, currentState, serverStateFactory);
            server.electionTimeout();

            verify(serverStateFactory).createCandidate(TERM_1);
        }

        @Test
        public void willVoteForSelf() {
            var server = new Server<>(SERVER_ID, currentState, serverStateFactory);
            server.electionTimeout();

            verify(candidate).recordVoteAndClaimLeadershipIfEligible(SERVER_ID);
        }

        @Test
        public void willBroadcastRequestVoteRequests() {
            var server = new Server<>(SERVER_ID, currentState, serverStateFactory);
            server.electionTimeout();

            verify(candidate).requestVotes();
        }

        @Test
        public void willTransitionToLeader_WhenOwnVoteIsQuorum() {
            when(leader.getServerStateType()).thenReturn(LEADER);
            when(candidate.recordVoteAndClaimLeadershipIfEligible(SERVER_ID)).thenReturn(complete(leader));

            var server = new Server<>(SERVER_ID, currentState, serverStateFactory);
            server.electionTimeout();

            assertThat(server.getState()).isEqualTo(LEADER);
        }

        @Test
        public void willNotRequestVotes_WhenOwnVoteCausesTransitionToLeaderState() {
            when(candidate.recordVoteAndClaimLeadershipIfEligible(SERVER_ID)).thenReturn(complete(leader));

            var server = new Server<>(SERVER_ID, currentState, serverStateFactory);
            server.electionTimeout();

            verify(candidate, never()).requestVotes();
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
            var server = new Server<>(SERVER_ID, serverState, serverStateFactory);
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

            new Server<>(SERVER_ID, serverState, serverStateFactory).handle(rpcMessage);

            verify(serverState).handle(rpcMessage);
            verifyNoMoreInteractions(serverState);
        }

        @Test
        void willTransitionToNextStateAndDisposeOfPrevious() {
            when(serverState.handle(rpcMessage)).thenReturn(complete(nextServerState));
            when(nextServerState.handle(rpcMessage)).thenReturn(complete(nextServerState));

            Server<Long> server = new Server<>(SERVER_ID, serverState, serverStateFactory);

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

            Server<Long> server = new Server<>(SERVER_ID, serverState, serverStateFactory);

            server.handle(rpcMessage);
            verify(serverState).handle(rpcMessage);
            verify(serverState).dispose();
            verify(nextServerState).handle(rpcMessage);

            verifyNoMoreInteractions(serverState, nextServerState);
        }
    }
}