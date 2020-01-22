package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class FollowerTest {

    private static final long SERVER_ID = 100L;
    private static final long LEADER_SERVER_ID = 101L;
    private static final long NON_LEADER_SERVER_ID = 102L;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final int CLIENT_ID = 123;
    private static final LogEntry ENTRY_1 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, 0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new StateMachineCommandEntry(TERM_0, CLIENT_ID, 1, "second".getBytes());
    private static final LogEntry ENTRY_3 = new StateMachineCommandEntry(TERM_1, CLIENT_ID, 2, "third".getBytes());

    @Mock
    private Cluster<Long> cluster;
    @Mock
    private ServerStateFactory<Long> serverStateFactory;

    @Nested
    class HandleAppendEntriesRequest {

        @Test
        public void handleAppendEntriesRequest_WillRejectRequest_WhenLeaderTermIsLessThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_0, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), emptyList(), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, false, Optional.empty());
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        public void handleAppendEntriesRequest_WillRejectRequest_PrevLogEntryHasIncorrectTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_1), emptyList(), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, false, Optional.empty());
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        public void handleAppendEntriesRequest_WillAcceptRequest_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        public void handleAppendEntriesRequest_WillThrowIllegalStateException_WhenLeaderTermIsGreaterThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> followerState.handle(new AppendEntriesRequest<>(TERM_2, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0)));
            assertThat(ex.getMessage()).isEqualTo("Received a request from a future term! this should never happen");
        }

        @Test
        public void handleAppendEntriesRequest_WillAcceptRequest_AndAdvanceCommitIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLocal() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            Follower<Long> followerState = new Follower<>(TERM_1, null, log, cluster, serverStateFactory, LEADER_SERVER_ID);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 2));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            assertThat(log.getCommitIndex()).isEqualTo(2);
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        public void handleAppendEntriesRequest_WillAcceptRequest_AndAdvanceCommitIndexToLastLocalIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLastLocalIndex() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            Follower<Long> followerState = new Follower<>(TERM_1, null, log, cluster, serverStateFactory, LEADER_SERVER_ID);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 10));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            assertThat(log.getCommitIndex()).isEqualTo(3);
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        public void handleAppendEntriesRequest_WillReturnLastAppendedIndex_WhenAppendIsSuccessful() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, LEADER_SERVER_ID);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 1, Optional.of(TERM_0), List.of(ENTRY_2), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(2));
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        public void handleAppendEntriesRequest_WillRejectRequest__WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndSenderIsNotTheCurrentLeader() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, NON_LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, NON_LEADER_SERVER_ID, false, Optional.empty());
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }
    }
}