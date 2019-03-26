package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.serverstates.Result.incomplete;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.RegisterClientRequest;
import au.id.tindall.distalg.raft.rpc.RegisterClientResponse;
import au.id.tindall.distalg.raft.rpc.RegisterClientStatus;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;
import au.id.tindall.distalg.raft.rpc.RpcMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ServerStateTest {

    private static final long SERVER_ID = 100L;
    private static final long OTHER_SERVER_ID = 101L;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final Term TERM_3 = new Term(3);
    private static final LogEntry ENTRY_1 = new StateMachineCommandEntry(TERM_0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new StateMachineCommandEntry(TERM_0, "second".getBytes());
    private static final LogEntry ENTRY_3 = new StateMachineCommandEntry(TERM_1, "third".getBytes());

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private RpcMessage<Long> rpcMessage;

    @Nested
    class HandleRequest {

        @Test
        void willRevertToFollowerStateAndResetVotedForAndAdvanceTerm_WhenSenderTermIsGreaterThanLocalTerm() {
            var localTerm = TERM_1;
            var messageTerm = localTerm.next();
            when(rpcMessage.getTerm()).thenReturn(messageTerm);
            var log = logContaining();
            var serverState = new ServerStateImpl(SERVER_ID, localTerm, OTHER_SERVER_ID, log, cluster);
            assertThat(serverState.handle(rpcMessage))
                    .isEqualToComparingFieldByFieldRecursively(incomplete(new Follower<>(SERVER_ID, messageTerm, null, log, cluster)));
        }
    }

    @Nested
    class HandleRequestVoteRequest {

        @Test
        public void willNotGrantVote_WhenRequesterTermIsLowerThanLocalTerm() {
            var serverState = new ServerStateImpl(SERVER_ID, TERM_3, null, logContaining(), cluster);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 100, Optional.of(TERM_2)));
            verify(cluster).send(refEq(new RequestVoteResponse<>(TERM_3, SERVER_ID, OTHER_SERVER_ID, false)));
            assertThat(serverState.getVotedFor()).isEmpty();
        }

        @Test
        public void willNotGrantVote_WhenServerHasAlreadyVoted() {
            var serverState = new ServerStateImpl(SERVER_ID, TERM_3, SERVER_ID, logContaining(), cluster);
            serverState.handle(new RequestVoteRequest<>(TERM_3, OTHER_SERVER_ID, 100, Optional.of(TERM_2)));
            verify(cluster).send(refEq(new RequestVoteResponse<>(TERM_3, SERVER_ID, OTHER_SERVER_ID, false)));
            assertThat(serverState.getVotedFor()).contains(SERVER_ID);
        }

        @Test
        public void willNotGrantVote_WhenServerLogIsMoreUpToDateThanRequesterLog() {
            var serverState = new ServerStateImpl(SERVER_ID, TERM_2, null, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 2, Optional.of(TERM_0)));
            verify(cluster).send(refEq(new RequestVoteResponse<>(TERM_2, SERVER_ID, OTHER_SERVER_ID, false)));
            assertThat(serverState.getVotedFor()).isEmpty();
        }

        @Test
        public void willGrantVote_WhenRequesterTermIsEqualLogsAreSameAndServerHasNotAlreadyVoted() {
            var serverState = new ServerStateImpl(SERVER_ID, TERM_2, null, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1)));
            verify(cluster).send(refEq(new RequestVoteResponse<>(TERM_2, SERVER_ID, OTHER_SERVER_ID, true)));
            assertThat(serverState.getVotedFor()).contains(OTHER_SERVER_ID);
        }

        @Test
        public void willGrantVote_WhenRequesterTermIsEqualServerLogIsLessUpToDateAndServerHasNotAlreadyVoted() {
            var serverState = new ServerStateImpl(SERVER_ID, TERM_2, null, logContaining(ENTRY_1, ENTRY_2), cluster);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1)));
            verify(cluster).send(refEq(new RequestVoteResponse<>(TERM_2, SERVER_ID, OTHER_SERVER_ID, true)));
            assertThat(serverState.getVotedFor()).contains(OTHER_SERVER_ID);
        }

        @Test
        public void willGrantVote_WhenRequesterTermIsEqualLogsAreSameAndServerHasAlreadyVotedForRequester() {
            var serverState = new ServerStateImpl(SERVER_ID, TERM_2, OTHER_SERVER_ID, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
            serverState.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1)));
            verify(cluster).send(refEq(new RequestVoteResponse<>(TERM_2, SERVER_ID, OTHER_SERVER_ID, true)));
            assertThat(serverState.getVotedFor()).contains(OTHER_SERVER_ID);
        }
    }

    @Nested
    class HandleRegisterClientRequest {

        @Test
        @SuppressWarnings("unchecked")
        void willReturnNotLeader() throws ExecutionException, InterruptedException {
            var serverState = new ServerStateImpl(SERVER_ID, TERM_0, null, logContaining(), cluster);
            CompletableFuture<RegisterClientResponse<Long>> handle = serverState.handle(new RegisterClientRequest(SERVER_ID));
            assertThat(handle).isCompleted();
            assertThat(handle.get()).usingRecursiveComparison()
                    .isEqualTo(new RegisterClientResponse<>(RegisterClientStatus.NOT_LEADER, null, null));
        }
    }

    private static class ServerStateImpl extends ServerState<Long> {

        ServerStateImpl(Long serializable, Term currentTerm, Long votedFor, Log log, Cluster<Long> cluster) {
            super(serializable, currentTerm, votedFor, log, cluster);
        }

        @Override
        public ServerStateType getServerStateType() {
            throw new UnsupportedOperationException("This is going away at some point I reckon");
        }
    }
}
