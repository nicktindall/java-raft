package au.id.tindall.distalg.raft.serverstates;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.serverstates.Result.complete;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Optional;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.LogEntry;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.AppendEntriesResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FollowerTest {

    private static final long SERVER_ID = 100L;
    private static final long OTHER_SERVER_ID = 101L;
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final LogEntry ENTRY_1 = new LogEntry(TERM_0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new LogEntry(TERM_0, "second".getBytes());
    private static final LogEntry ENTRY_3 = new LogEntry(TERM_1, "third".getBytes());

    @Mock
    private Cluster<Long> cluster;

    @Test
    public void handleAppendEntriesRequest_WillRejectRequest_WhenLeaderTermIsLessThanLocalTerm() {
        Follower<Long> followerState = new Follower<>(SERVER_ID, TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_0, OTHER_SERVER_ID, 2, Optional.of(TERM_0), emptyList(), 0));
        verify(cluster).send(refEq(new AppendEntriesResponse<>(TERM_1, SERVER_ID, OTHER_SERVER_ID, false, Optional.empty())));
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
    }

    @Test
    public void handleAppendEntriesRequest_WillRejectRequest_PrevLogEntryHasIncorrectTerm() {
        Follower<Long> followerState = new Follower<>(SERVER_ID, TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, OTHER_SERVER_ID, 2, Optional.of(TERM_1), emptyList(), 0));
        verify(cluster).send(refEq(new AppendEntriesResponse<>(TERM_1, SERVER_ID, OTHER_SERVER_ID, false, Optional.empty())));
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
    }

    @Test
    public void handleAppendEntriesRequest_WillAcceptRequest_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm() {
        Follower<Long> followerState = new Follower<>(SERVER_ID, TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, OTHER_SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
        verify(cluster).send(refEq(new AppendEntriesResponse<>(TERM_1, SERVER_ID, OTHER_SERVER_ID, true, Optional.of(3))));
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
    }

    @Test
    public void handleAppendEntriesRequest_WillAcceptRequest_AndAdvanceTerm_WhenPreviousLogIndexMatches_AndLeaderTermIsGreaterThanLocalTerm() {
        Follower<Long> followerState = new Follower<>(SERVER_ID, TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_2, OTHER_SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 0));
        verify(cluster).send(refEq(new AppendEntriesResponse<>(TERM_2, SERVER_ID, OTHER_SERVER_ID, true, Optional.of(3))));
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
    }

    @Test
    public void handleAppendEntriesRequest_WillAcceptRequest_AndAdvanceCommitIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLocal() {
        Follower<Long> followerState = new Follower<>(SERVER_ID, TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, OTHER_SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 2));
        verify(cluster).send(refEq(new AppendEntriesResponse<>(TERM_1, SERVER_ID, OTHER_SERVER_ID, true, Optional.of(3))));
        assertThat(followerState.getCommitIndex()).isEqualTo(2);
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
    }

    @Test
    public void handleAppendEntriesRequest_WillAcceptRequest_AndAdvanceCommitIndexToLastLocalIndex_WhenPreviousLogIndexMatches_AndLeaderTermIsEqualToLocalTerm_AndCommitIndexIsGreaterThanLastLocalIndex() {
        Follower<Long> followerState = new Follower<>(SERVER_ID, TERM_1, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_1, OTHER_SERVER_ID, 2, Optional.of(TERM_0), singletonList(ENTRY_3), 10));
        verify(cluster).send(refEq(new AppendEntriesResponse<>(TERM_1, SERVER_ID, OTHER_SERVER_ID, true, Optional.of(3))));
        assertThat(followerState.getCommitIndex()).isEqualTo(3);
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
    }

    @Test
    public void handleAppendEntriesRequest_WillReturnLastAppendedIndex_WhenAppendIsSuccessful() {
        Follower<Long> followerState = new Follower<>(SERVER_ID, TERM_1, null, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        Result<Long> result = followerState.handle(new AppendEntriesRequest<>(TERM_2, OTHER_SERVER_ID, 1, Optional.of(TERM_0), List.of(ENTRY_2), 0));
        verify(cluster).send(refEq(new AppendEntriesResponse<>(TERM_2, SERVER_ID, OTHER_SERVER_ID, true, Optional.of(2))));
        assertThat(result).isEqualToComparingFieldByFieldRecursively(complete(followerState));
    }
}