package au.id.tindall.distalg.raft;

import static au.id.tindall.distalg.raft.DomainUtils.logContaining;
import static au.id.tindall.distalg.raft.ServerState.CANDIDATE;
import static au.id.tindall.distalg.raft.ServerState.FOLLOWER;
import static au.id.tindall.distalg.raft.ServerState.LEADER;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import au.id.tindall.distalg.raft.comms.Cluster;
import au.id.tindall.distalg.raft.log.Log;
import au.id.tindall.distalg.raft.log.LogEntry;
import au.id.tindall.distalg.raft.log.Term;
import au.id.tindall.distalg.raft.rpc.AppendEntriesRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteRequest;
import au.id.tindall.distalg.raft.rpc.RequestVoteResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerTest {

    private static final long SERVER_ID = 100L;
    private static final long OTHER_SERVER_ID = 101L;
    private static final Term RESTORED_TERM = new Term(111);
    private static final long RESTORED_VOTED_FOR = 999L;
    private static final Log RESTORED_LOG = new Log();
    private static final Term TERM_0 = new Term(0);
    private static final Term TERM_1 = new Term(1);
    private static final Term TERM_2 = new Term(2);
    private static final Term TERM_3 = new Term(3);
    private static final LogEntry ENTRY_1 = new LogEntry(TERM_0, "first".getBytes());
    private static final LogEntry ENTRY_2 = new LogEntry(TERM_0, "second".getBytes());
    private static final LogEntry ENTRY_3 = new LogEntry(TERM_1, "third".getBytes());

    @Mock
    private Cluster<Long> cluster;

    @Test
    public void newServerConstructor_WillSetId() {
        Server<Long> server = new Server<>(SERVER_ID, cluster);
        assertThat(server.getId()).isEqualTo(SERVER_ID);
    }

    @Test
    public void newServerConstructor_WillInitializeCurrentTermToZero() {
        Server<Long> server = new Server<>(SERVER_ID, cluster);
        assertThat(server.getCurrentTerm()).isEqualTo(new Term(0));
    }

    @Test
    public void newServerConstructor_WillInitializeLogToEmpty() {
        Server<Long> server = new Server<>(SERVER_ID, cluster);
        assertThat(server.getLog().getEntries()).isEmpty();
    }

    @Test
    public void newServerConstructor_WillInitializeVotedForToNull() {
        Server<Long> server = new Server<>(SERVER_ID, cluster);
        assertThat(server.getVotedFor()).isEmpty();
    }

    @Test
    public void newServerConstructor_WillInitializeStateToFollower() {
        Server<Long> server = new Server<>(SERVER_ID, cluster);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    public void resetServerConstructor_WillSetId() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        assertThat(server.getId()).isEqualTo(SERVER_ID);
    }

    @Test
    public void resetServerConstructor_WillRestoreCurrentTerm() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        assertThat(server.getCurrentTerm()).isEqualTo(RESTORED_TERM);
    }

    @Test
    public void resetServerConstructor_WillRestoreLog() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        assertThat(server.getLog()).isSameAs(RESTORED_LOG);
    }

    @Test
    public void resetServerConstructor_WillRestoreVotedFor() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        assertThat(server.getVotedFor()).contains(RESTORED_VOTED_FOR);
    }

    @Test
    public void resetServerConstructor_WillInitializeStateToFollower() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
    }

    @Test
    public void electionTimeout_WillSetStateToCandidate() {
        Server<Long> server = new Server<>(SERVER_ID, cluster);
        server.electionTimeout();
        assertThat(server.getState()).isEqualTo(CANDIDATE);
    }

    @Test
    public void electionTimeout_WillIncrementTerm() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        server.electionTimeout();
        assertThat(server.getCurrentTerm()).isEqualTo(RESTORED_TERM.next());
    }

    @Test
    public void electionTimeout_WillSetVotedForToNull() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        server.electionTimeout();
        assertThat(server.getVotedFor()).isEmpty();
    }

    @Test
    public void electionTimeout_WillBroadcastRequestVoteRequests() {
        Server<Long> server = new Server<>(SERVER_ID, RESTORED_TERM, RESTORED_VOTED_FOR, RESTORED_LOG, cluster);
        server.electionTimeout();
        verify(cluster).broadcastMessage(refEq(new RequestVoteRequest<>(RESTORED_TERM.next(), SERVER_ID, RESTORED_LOG.getLastLogIndex(), RESTORED_LOG.getLastLogTerm())));
    }

    @Test
    public void handleRequestVote_WillNotGrantVote_WhenRequesterTermIsLowerThanLocalTerm() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_3, null, new Log(), cluster);
        assertThat(server.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 100, Optional.of(TERM_2))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_3, false));
        assertThat(server.getVotedFor()).isEmpty();
    }

    @Test
    public void handleRequestVote_WillNotGrantVote_WhenServerHasAlreadyVoted() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_3, SERVER_ID, new Log(), cluster);
        assertThat(server.handle(new RequestVoteRequest<>(TERM_3, OTHER_SERVER_ID, 100, Optional.of(TERM_2))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_3, false));
        assertThat(server.getVotedFor()).contains(SERVER_ID);
    }

    @Test
    public void handleRequestVote_WillNotGrantVote_WhenServerLogIsMoreUpToDateThanRequesterLog() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, null, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        assertThat(server.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 2, Optional.of(TERM_0))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_2, false));
        assertThat(server.getVotedFor()).isEmpty();
    }

    @Test
    public void handleRequestVote_WillGrantVote_WhenRequesterTermIsEqualLogsAreSameAndServerHasNotAlreadyVoted() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, null, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        assertThat(server.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_2, true));
        assertThat(server.getVotedFor()).contains(OTHER_SERVER_ID);
    }

    @Test
    public void handleRequestVote_WillGrantVote_WhenRequesterTermIsEqualServerLogIsLessUpToDateAndServerHasNotAlreadyVoted() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        assertThat(server.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_2, true));
        assertThat(server.getVotedFor()).contains(OTHER_SERVER_ID);
    }

    @Test
    public void handleRequestVote_WillGrantVote_WhenRequesterTermIsEqualLogsAreSameAndServerHasAlreadyVotedForRequester() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, OTHER_SERVER_ID, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        assertThat(server.handle(new RequestVoteRequest<>(TERM_2, OTHER_SERVER_ID, 3, Optional.of(TERM_1))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_2, true));
        assertThat(server.getVotedFor()).contains(OTHER_SERVER_ID);
    }

    @Test
    public void handleRequestVote_WillGrantVoteAndAdvanceTerm_WhenRequesterTermIsGreaterLogsAreSameAndServerHasAlreadyVoted() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, SERVER_ID, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        assertThat(server.handle(new RequestVoteRequest<>(TERM_3, OTHER_SERVER_ID, 3, Optional.of(TERM_1))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_3, true));
        assertThat(server.getVotedFor()).contains(OTHER_SERVER_ID);
    }

    @Test
    public void handleRequestVote_WillRevertStateToFollower_WhenRequesterTermIsGreaterLogsAreSameAndServerHasAlreadyVoted() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_1, SERVER_ID, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        server.electionTimeout();
        assertThat(server.handle(new RequestVoteRequest<>(TERM_3, OTHER_SERVER_ID, 3, Optional.of(TERM_1))))
                .isEqualToComparingFieldByFieldRecursively(new RequestVoteResponse<>(SERVER_ID, TERM_3, true));
        assertThat(server.getState()).isEqualTo(FOLLOWER);
        assertThat(server.getCurrentTerm()).isEqualTo(TERM_3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getReceivedVotes_WillReturnUnmodifiableSet() {
        Server<Long> server = new Server<>(SERVER_ID, cluster);
        server.getReceivedVotes().add(OTHER_SERVER_ID);
    }

    @Test
    public void handleRequestVoteResponse_WillRecordVote_WhenResponseIsNotStaleAndQuorumIsNotReached() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, SERVER_ID, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        server.electionTimeout();
        when(cluster.isQuorum(anySet())).thenReturn(false);
        server.handle(new RequestVoteResponse<>(OTHER_SERVER_ID, TERM_3, true));
        verify(cluster, never()).broadcastMessage(any(AppendEntriesRequest.class));
        assertThat(server.getState()).isEqualTo(CANDIDATE);
        assertThat(server.getReceivedVotes()).contains(OTHER_SERVER_ID);
    }

    @Test
    public void handleRequestVoteResponse_WillTransitionToLeaderStateAndSendHeartbeat_WhenAQuorumIsReached() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, SERVER_ID, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        server.electionTimeout();
        when(cluster.isQuorum(Set.of(SERVER_ID))).thenReturn(true);
        server.handle(new RequestVoteResponse<>(SERVER_ID, TERM_3, true));
        verify(cluster).broadcastMessage(refEq(new AppendEntriesRequest<>(TERM_3, SERVER_ID, 3, Optional.of(TERM_1), emptyList(), 0)));
        assertThat(server.getState()).isEqualTo(LEADER);
    }

    @Test
    public void handleRequestVoteResponse_WillIgnoreResponse_WhenItIsStale() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_2, SERVER_ID, logContaining(ENTRY_1, ENTRY_2, ENTRY_3), cluster);
        server.electionTimeout();
        server.handle(new RequestVoteResponse<>(OTHER_SERVER_ID, TERM_2, true));
        verify(cluster, never()).broadcastMessage(any(AppendEntriesRequest.class));
        verify(cluster, never()).isQuorum(anySet());
        assertThat(server.getReceivedVotes()).doesNotContain(OTHER_SERVER_ID);
        assertThat(server.getState()).isEqualTo(CANDIDATE);
    }

    @Test
    public void handleRequestVoteResponse_WillRevertToFollowerStateAndClearVotedFor_WhenResponseHasNewerTermThanServer() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_0, SERVER_ID, logContaining(ENTRY_1, ENTRY_2), cluster);
        server.electionTimeout();
        server.handle(new RequestVoteResponse<>(OTHER_SERVER_ID, TERM_2, false));
        verify(cluster, never()).broadcastMessage(any(AppendEntriesRequest.class));
        verify(cluster, never()).isQuorum(anySet());
        assertThat(server.getReceivedVotes()).doesNotContain(OTHER_SERVER_ID);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
        assertThat(server.getVotedFor()).isEmpty();
    }

    @Test
    public void handleRequestVoteResponse_WillNotAttemptToBecomeLeader_WhenStateIsNotCandidate() {
        Server<Long> server = new Server<>(SERVER_ID, TERM_0, null, logContaining(ENTRY_1, ENTRY_2), cluster);
        server.handle(new RequestVoteResponse<>(OTHER_SERVER_ID, TERM_0, true));
        verify(cluster, never()).isQuorum(anySet());
        assertThat(server.getReceivedVotes()).contains(OTHER_SERVER_ID);
        assertThat(server.getState()).isEqualTo(FOLLOWER);
        assertThat(server.getVotedFor()).isEmpty();
    }
}