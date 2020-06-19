package au.id.tindall.distalg.raft.serverstates;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.driver.ElectionScheduler;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.log.entries.LogEntry;
import au.id.tindall.distalg.raft.log.entries.StateMachineCommandEntry;
import au.id.tindall.distalg.raft.rpc.server.AppendEntriesRequest;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class FollowerTest {

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
    @Mock
    private ElectionScheduler<Long> electionScheduler;

    @Nested
    class OnEnterState {

        Follower<Long> followerState;

        @BeforeEach
        void setUp() {
            followerState = new Follower<>(TERM_2, null, logContaining(), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            followerState.enterState();
        }

        @Test
        void willStartElectionTimeouts() {
            verify(electionScheduler).startTimeouts();
        }
    }

    @Nested
    class OnLeaveState {

        Follower<Long> followerState;

        @BeforeEach
        void setUp() {
            followerState = new Follower<>(TERM_2, null, logContaining(), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            followerState.leaveState();
        }

        @Test
        void willStartElectionTimeouts() {
            verify(electionScheduler).stopTimeouts();
        }
    }

    @Nested
    class HandleAppendEntriesRequest {

        @Test
        void willRejectRequest_WhenLeaderTermIsLessThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_0, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), emptyList(), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, false, Optional.empty());
            verifyNoInteractions(electionScheduler);
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        void willRejectRequest_PrevLogEntryHasIncorrectTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_1), emptyList(), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, false, Optional.empty());
            verify(electionScheduler).resetTimeout();
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        void willAcceptRequest_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            verify(electionScheduler).resetTimeout();
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        void willThrowIllegalStateException_WhenLeaderTermIsGreaterThanLocalTerm() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> followerState.handle(new AppendEntriesRequest<>(TERM_2, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0)));
            verifyNoInteractions(electionScheduler);
            assertThat(ex.getMessage()).isEqualTo("Received a request from a future term! this should never happen");
        }

        @Test
        void willAcceptRequest_AndAdvanceCommitIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLocal() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            Follower<Long> followerState = new Follower<>(TERM_1, null, log, cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 2));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            verify(electionScheduler).resetTimeout();
            assertThat(log.getCommitIndex()).isEqualTo(2);
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        void willAcceptRequest_AndAdvanceCommitIndexToLastLocalIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLastLocalIndex() {
            Log log = logContaining(ENTRY_1, ENTRY_2);
            Follower<Long> followerState = new Follower<>(TERM_1, null, log, cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 10));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(3));
            verify(electionScheduler).resetTimeout();
            assertThat(log.getCommitIndex()).isEqualTo(3);
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        void willReturnLastAppendedIndex_WhenAppendIsSuccessful() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, LEADER_SERVER_ID, SERVER_ID, 1, Optional.of(TERM_0), List.of(ENTRY_2), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, LEADER_SERVER_ID, true, Optional.of(2));
            verify(electionScheduler).resetTimeout();
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }

        @Test
        void willRejectRequest_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndSenderIsNotTheCurrentLeader() {
            Follower<Long> followerState = new Follower<>(TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster, serverStateFactory, LEADER_SERVER_ID, electionScheduler);
            Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, NON_LEADER_SERVER_ID, SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
            verify(cluster).sendAppendEntriesResponse(TERM_1, NON_LEADER_SERVER_ID, false, Optional.empty());
            verifyNoInteractions(electionScheduler);
            assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
        }
    }
}